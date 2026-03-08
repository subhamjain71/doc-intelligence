from flask import Flask,request,jsonify
import psycopg2
import os
from minio import Minio
from minio.error import S3Error
import datetime
import uuid
import json

DB_URL = os.getenv("DATABASE_URL","")
MINIO_URL = os.getenv("MINIO_URL","")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER","")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD","")
MINIO_DEFAULT_BUCKETS = os.getenv("MINIO_DEFAULT_BUCKETS","")
MINIO_EXTERNAL_URL = os.getenv('MINIO_EXTERNAL_URL','')

app = Flask(__name__)
def get_connection():
    return psycopg2.connect(DB_URL)

minio_client = Minio(
        MINIO_URL,
        access_key=MINIO_ROOT_USER,  # Replace with your access key
        secret_key=MINIO_ROOT_PASSWORD,  # Replace with your secret key
        secure=False  # Set to True if using HTTPS
)

@app.route("/health")
def health():
    return jsonify({"status":"ok"}),200

@app.route("/",methods = ["POST"])
def upload():
    user_id = request.headers.get("X-User-Id","")
    if not user_id:
        return jsonify({'error':'user_id not found'}),400
    
    print(f"User {user_id} uploading a file.")
    data = request.get_json()
    filename = data.get("filename","")
    size = data.get("size","")
    s3_key = f'users/{user_id}/{uuid.uuid4()}/{filename}'

    try:
        bucket_name = os.getenv("MINIO_DEFAULT_BUCKETS", "mybucket")
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"Bucket {bucket_name} created successfully.")
        else:
            print(f"Bucket {bucket_name} already exists.")
    except S3Error as e:
        return jsonify({"error":f"Error connecting to Minio: {e}"}),500
    presigned_url = minio_client.presigned_put_object(
        bucket_name=MINIO_DEFAULT_BUCKETS,
        object_name = s3_key,
        expires=datetime.timedelta(hours=2),
    )
    print('presigned_url',presigned_url)
    presigned_url = presigned_url.replace('minio:9000', 'localhost:9090')
    presigned_expiry = datetime.datetime.utcnow() + datetime.timedelta(hours=2)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO FILES(user_id,filename,s3_key,size_bytes,presigned_expiry,status) VALUES(%s,%s,%s,%s,%s,%s) returning id',
            (user_id,filename,s3_key,size,presigned_expiry,'pending_upload')
        )
        result = cur.fetchone()
        conn.commit()
        print(f"Presigned Url Created for file {filename}")
        return jsonify({"id":result[0],"presigned_url":presigned_url,"presigned_expiry" : presigned_expiry}),201
    except Exception as e:
        print(f'Error: {e}')
        conn.rollback()
        return jsonify({"error":f'something went wrong please try again : {str(e)}'})
    finally:
        conn.close()


@app.route("/")
def get_files():
    user_id = request.headers.get("X-User-Id","")
    if not user_id:
        return jsonify({'error':'user_id not found'}),400
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            'select id,filename,size_bytes,status from files where user_id=%s and is_deleted=false',(user_id,)
        )
        results= cur.fetchall()
        conn.commit()
        return jsonify(
            [{
                'id':result[0],
                'filename':result[1],
                'size': result[2],
                'status':result[3]
            } for result in results]
        )
    except Exception as e:
        print(f'Error: {e}')
        conn.rollback()
        return jsonify({"error":f'something went wrong please try again : {str(e)}'})
    finally:
        conn.close()


@app.route("/<string:id>")
def get_file(id):
    user_id = request.headers.get("X-User-Id","")
    if not user_id:
        return jsonify({'error':'user_id not found'}),400
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            'select id,filename,size_bytes,status from files where id=%s and user_id=%s and is_deleted=false',(id,user_id,)
        )
        result= cur.fetchone()
        conn.commit()
        return jsonify(
            {
                'id':result[0],
                'filename':result[1],
                'size': result[2],
                'status':result[3]
            }
        )
    except Exception as e:
        print(f'Error: {e}')
        conn.rollback()
        return jsonify({"error":f'something went wrong please try again : {str(e)}'})
    finally:
        conn.close()

@app.route("/<string:id>", methods = ['DELETE'])
def delete_file(id):
    user_id = request.headers.get("X-User-Id","")
    if not user_id:
        return jsonify({'error':'user_id not found'}),400
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            'UPDATE files SET is_deleted=true, deleted_at = NOW() where id=%s and user_id=%s and is_deleted=false',(id,user_id,)
        )
        cur.execute(
            'INSERT INTO outbox(topic,payload) VALUES(%s,%s)',('file.deleted',json.dumps({"file_id":id})) 
        )

        cur.execute(
            'UPDATE embeddings SET is_deleted=true  where file_id=%s and user_id=%s and is_deleted=false',(id,user_id,)
        )
        conn.commit()
        return jsonify(
            {
                'id':id,
                'is_deleted':True
            }
        )
    except Exception as e:
        print(f'Error: {e}')
        conn.rollback()
        return jsonify({"error":f'something went wrong please try again : {str(e)}'})
    finally:
        conn.close()

@app.route("/<string:id>/complete",methods=["POST"])
def mark_complete(id):
    user_id = request.headers.get("X-User-Id","")
    if not user_id:
        return jsonify({'error':'user_id not found'}),400
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            'UPDATE files SET status = %s  where id=%s and user_id=%s and is_deleted=false and status=%s RETURNING id',('uploaded',id,user_id,"pending_upload")
        )
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'file not found or already processed'}), 404
        cur.execute(
            'INSERT INTO outbox(topic,payload) VALUES(%s,%s)',('file.uploaded',json.dumps({"file_id":id})) 
        )

        conn.commit()
        return jsonify({"file_id":id,"status":'uploaded'})
    except Exception as e:
        print(f'Error: {e}')
        conn.rollback()
        return jsonify({"error":f'something went wrong please try again : {str(e)}'})
    finally:
        conn.close()



if __name__ == '__main__':
    app.run(host="0.0.0.0",port=8002)

