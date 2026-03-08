from flask import Flask,jsonify,request,stream_with_context,Response
import psycopg2
import os
import redis
import requests
import json
import time

DB_URL = os.getenv("DATABASE_URL")
OLLAMA = os.getenv("OLLAMA_URL")

r = redis.from_url(os.environ.get('REDIS_URL', 'redis://redis:6379'), decode_responses=True)
con = psycopg2.connect(DB_URL)

app = Flask(__name__)

@app.route("/health")
def health():
    return jsonify({"status":"ok"})

@app.route("/conversations",methods = ['POST'])
def conversation():
    cur = con.cursor()
    user_id = request.headers.get("X-User-Id","")
    if not user_id:
        return jsonify({'error':'user_id not found'}),400
    try:
        cur.execute('INSERT INTO conversations(user_id) VALUES (%s) returning id',(user_id,))
        id = cur.fetchone()[0]
        con.commit()
        return jsonify({"conversationId":id})

    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route("/conversations",methods = ['GET'])
def get_conversation():
    cur = con.cursor()
    user_id = request.headers.get("X-User-Id","")
    if not user_id:
        return jsonify({'error':'user_id not found'}),400
    try:
        cur.execute('SELECT id,user_id,create_at from conversations where user_id=%s',(user_id,))
        rows = cur.fetchall()
        return jsonify({"conversations":[{"id":row[0],"user_id":row[1],"created_at": row[2]} for row in rows]})

    except Exception as e:
        return jsonify({"error":str(e)}),500
    

@app.route("/conversations/<int:conversationId>/messages",methods = ['GET'])
def get_messages(conversationId):
    cur = con.cursor()
    user_id = request.headers.get("X-User-Id","")
    if not user_id:
        return jsonify({'error':'user_id not found'}),400
    try:
        cur.execute('SELECT m.id,m.conversation_id,m.content,m.created_at,m.role from messages m JOIN conversations c ON c.id = m.conversation_id where m.conversation_id=%s and c.user_id=%s ORDER BY m.created_at ASC',(conversationId,user_id))
        rows = cur.fetchall()
        return jsonify({"messages":[{"id":row[0],"conversation_id":row[1],"content": row[2],"created_at":row[3],"role":row[4]} for row in rows]})

    except Exception as e:
        return jsonify({"error":str(e)})
def embed(question):
    response= requests.post(f'{OLLAMA}/api/embeddings',json={'model':'nomic-embed-text','prompt':question},timeout=15)
    return response.json()['embedding']
HISTORY_KEY = lambda cid: f'conv:{cid}:messages'



def get_history(conversationId):
    key = HISTORY_KEY(conversationId)
    messages = r.lrange(key,-20,-1)
    if messages:
        return [json.loads(m) for m in messages]
    
    cur = con.cursor()
    cur.execute(
        '''
SELECT  role,content from messages where conversation_id = %s order by created_at DESC LIMIT 20

    ''',(conversationId,)
    )
    rows = list(reversed(cur.fetchall()))
    history = [{"role":r[0],"content":r[1]} for r in rows]
    if history:
        pipe = r.pipeline()
        for msg in history:
            pipe.rpush(key,json.dumps(msg))
        pipe.expire(key,7200)
        pipe.execute()
    return history


def retrieve_chunks(user_id,embed_vector,limit=5):
    cur = con.cursor()
    cur.execute(
        '''
        select e.content,f.filename,e.page_number,1-(e.embedding <=> %s::vector) as similarity from embeddings e
        join files f on f.id = e.file_id where e.user_id=%s and e.is_deleted= FALSE order by e.embedding <=> %s::vector LIMIT %s''',
        (embed_vector,user_id,embed_vector,limit)

    )
    rows = cur.fetchall()
    return [(r[0],r[1],r[2],r[3]) for r in rows if r[3]>=0.4]

def trim_history(history,budget_tokens=3000):
    total = sum(len(m['content']) for m in history)
    while total > budget_tokens and len(history)>1:
        removed = history.pop(0)
        total-=len(removed['content'])
    return history

def save_message(conversationId,role,content):
    print("Saving", conversationId,role,content)
    cur = con.cursor()
    cur.execute(
        '''
INSERT INTO messages(conversation_id,role,content) VALUES(%s,%s,%s)
''',(conversationId,role,content)
    )
    con.commit()

    msg = {'role':role,'content':content}
    r.rpush(HISTORY_KEY(conversationId), json.dumps(msg))
    r.expire(HISTORY_KEY(conversationId),7200)




@app.route("/conversations/<int:conversationId>/query",methods=['POST'])
def query(conversationId):
    cur = con.cursor()
    user_id = request.headers.get("X-User-Id","")
    if not user_id:
        return jsonify({'error':'user_id not found'}),400   
    
    data = request.get_json()
    question = data.get("question", "")
    if not question:
        return jsonify({"error":'q parameter required'}),400
    print(question)
    def generate():
        try:
            q_vector = embed(question)
            print(q_vector)
            chunks = retrieve_chunks(user_id,q_vector)
            print(chunks)
            context = '\n\n'.join(
                [
                    f'[{filename}, page {page}]\n{content}' for content,filename,page,_ in chunks
                ]
            )
            print(context)
            if not context:
                yield f'data:{json.dumps({"token":"I could not found relevent information in doc"})}\n\n'
                time.sleep(2)
                yield f'data:{json.dumps({"done":"true"})}\n\n'
                return
            
            print(context)
            history = get_history(conversationId)
            history = trim_history(history)
            system = f''' you are a helpful assistant that answers questions based only on the provided document context.
            If the context does not contain enough information, say so clearly.

            Document context:
            {context}
            '''

            messages = history +[{'role':'user','content':question}]
            prompt = system + '\n\n' + '\n'.join([
                f"{m['role'].upper()}:{m['content']}" for m in messages]
            )

            full_response = ''
            res = requests.post(
                f'{OLLAMA}/api/generate',
                json = {'model':'llama3.2:1b','prompt':prompt,'stream':True},
                stream=True, timeout=300

            )

            for line in res.iter_lines():
                if not line: continue
                chunk = json.loads(line)
                token = chunk.get('response','')
                full_response+=token
                yield f'data:{json.dumps({"token":token})}\n\n'
                if chunk.get('done'): break
            
            save_message(conversationId,'user',question)
            save_message(conversationId,'assistant',full_response)
            yield f'data:{json.dumps({"done":True,"sources":[{"file":str(c[1]),"page":str(c[2])} for c in chunks]})}\n\n'

        except Exception as e:
            yield f'data:{json.dumps({"error": str(e)})}\n\n'
            return
    # Cache-Control : prevent proxies fro, buffering
    # X-Accel-Buffering:  for nginx to not do buffering
    # Stream_with_context: wrape generate with request context
    return Response(stream_with_context(generate()),mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache','X-Accel-Buffering':'no'})

if __name__=='__main__':
    app.run(host="0.0.0.0",port=8003,threaded=True)