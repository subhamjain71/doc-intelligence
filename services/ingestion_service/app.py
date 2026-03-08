
from confluent_kafka import Consumer
import os
import json
import psycopg2
from minio import Minio
import nltk
import requests
nltk.download('punkt',quiet=True)
nltk.download('punkt_tab',quiet=True)
from psycopg2.extras import execute_values

DB_URL = os.getenv("DATABASE_URL","")
MINIO_URL = os.getenv("MINIO_URL","")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER","")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD","")
MINIO_DEFAULT_BUCKETS = os.getenv("MINIO_DEFAULT_BUCKETS","")
OLLAMA = os.getenv("OLLAMA_URL")

c = Consumer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP'),
    'group.id': 'ingestion-worker',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})
minio_client = Minio(
        MINIO_URL,
        access_key=MINIO_ROOT_USER,  # Replace with your access key
        secret_key=MINIO_ROOT_PASSWORD,  # Replace with your secret key
        secure=False  # Set to True if using HTTPS
)


con = psycopg2.connect(os.getenv('DATABASE_URL'))
con.autocommit = False


def extract_text_from_pdf(pdf_bytes):
    import pypdf,io
    reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    pages = []
    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ''
        pages.append((i+1,text))
    return pages

def chunk_text(page_text,max_token=300):
    sentence = nltk.sent_tokenize(page_text)
    chunks,current,count = [],[],0
    for s in sentence:
        t = len(s.split())
        if count+t>max_token and current:
            chunks.append(''.join(current))
            current = current[-2:]
            count = sum(len(x.split()) for x in current)
        current.append(s)
        count+=t
    if current:
        chunks.append(' '.join(current))
    return chunks

def embed(text):
    res = requests.post(f'{OLLAMA}/api/embeddings',json = {'model':'nomic-embed-text','prompt':text},timeout=30)
    return res.json()['embedding']




def process_message(message):
    payload = json.loads(message)
    file_id = payload["file_id"]
    cur = con.cursor()
    cur.execute("UPDATE files SET status = %s ,processing_started_at=NOW() where id=%s and status=%s  returning s3_key,user_id ",('processing',file_id,'uploaded'))
    row = cur.fetchone()
    if not row:
        print("File was already processed")
        return
    s3_key = row[0]
    user_id=row[1]
    con.commit()
    try:
        pdf_bytes = minio_client.get_object(bucket_name=MINIO_DEFAULT_BUCKETS,object_name=s3_key).read()
        pages = extract_text_from_pdf(pdf_bytes)
        print(
            f"Worker has extracted {len(pages)} pages"
        )
        rows = []
        for page_num,page_text in pages:
            if not page_text.strip() : continue
            chunks = chunk_text(page_text)
            for i,chunk in enumerate(chunks):
                vector = embed(chunk)
                rows.append((file_id,user_id,i,chunk,page_num,vector))
        

        cur = con.cursor()

        # delete if already exists
        cur.execute('DELETE FROM embeddings where file_id = %s and user_id=%s',(file_id,user_id))
        execute_values(cur,
            '''INSERT INTO embeddings(file_id,user_id,chunk_index,content,page_number,embedding) VALUES %s''',
            rows,template='(%s,%s,%s,%s,%s,%s::vector)'
        )
        cur.execute(
            "Update files set status = 'processed' where id=%s",(file_id,)
        )
        con.commit()
        print(f'[WORKER] file {file_id} processed successfully')


    except Exception as e:
        con.rollback()
        print("Error while processing ", s3_key,e)

        return
    




c.subscribe(["file.uploaded"])
print("Ingestion Worker Started")
try:
    while True:
        msg = c.poll(2.0)
        if msg is None:
            print(f'Poll : None')
        elif msg.error():
            print(f'Poll : Error {msg.error()}')
        else:
            print(f'Poll : Got message: {msg.value()}')
            process_message(message=msg.value())
            c.commit(msg)



          
            
finally:
    c.close()