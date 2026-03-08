from flask import Flask,jsonify,request,Response,stream_with_context
import requests as req
import jwt
import os
import redis
from flask_cors import CORS



app = Flask(__name__)
CORS(app)
r = redis.from_url(os.environ.get('REDIS_URL', 'redis://redis:6379'), decode_responses=True)

SECRET = os.getenv("JWT_SECRET")

ROUTING = {
    "/api/auth":"http://user_service:8001",
    "/api/files":"http://upload_service:8002",
    "/api/query":"http://query_service:8003",
    "/api/admin":"http://admin_service:8004",
}
PUBLIC_PATHS = ['/api/auth/login', '/api/auth/register', '/health']

@app.route("/<path:path>",methods = ['GET','POST','PUT','DELETE'])
def handle(path):
    fullpath = "/"+path
    headers = {k: v for k, v in request.headers if k.lower() != 'host'}
    if fullpath not in PUBLIC_PATHS:
        auth_header = request.headers.get("Authorization","")
        if not auth_header.startswith("Bearer "):
            return jsonify({"error":"missing token"}),401
        
        token = auth_header[7:]
        try:
            token_decoded = jwt.decode(token, SECRET, algorithms=['HS256'])
            if r.sismember('banned_users',str(token_decoded["user_id"])):
                return jsonify({"error":"Account Suspended"}),401
            headers['X-User-ID'] = str(token_decoded["user_id"])
            
        except jwt.ExpiredSignatureError:
            return jsonify({"error":"token expired"}),401
        except jwt.InvalidTokenError:
            return jsonify({"error": "invalid token"}),401

    
    for prefix,upstream in ROUTING.items():
        if fullpath.startswith(prefix):
            upstream+=fullpath.replace(prefix,'',1)
            # TODO : handle SSE
            if request.headers.get('Accept') == 'text/event-stream':
                upstream_resp = req.request(
                    method=request.method,  # use the actual method — POST
                    url=upstream,
                    headers=headers,
                    data=request.data,  # forward the request body
                    stream=True,
                    timeout=None
                    )
                return Response(
                    stream_with_context(upstream_resp.iter_content(chunk_size=1)),
                    content_type='text/event-stream',
                    headers={
                        'Cache-Control': 'no-cache',
                        'X-Accel-Buffering': 'no'
                        })
                
            resp = req.request(method=request.method,url = upstream,data = request.data,headers=headers)
            return Response(resp.content,resp.status_code,resp.headers)
    return jsonify({"error": "not found"}),404


if __name__=='__main__':
    app.run(host="0.0.0.0",port = 8000)