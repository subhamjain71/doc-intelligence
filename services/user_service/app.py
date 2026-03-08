from flask import Flask,request,jsonify
import bcrypt,jwt,os,datetime,psycopg2

app = Flask(__name__)

DB_URL = os.getenv("DATABASE_URL")
SECRET = os.getenv("JWT_SECRET")


def get_connection():
    return psycopg2.connect(DB_URL)

@app.route("/register", methods = ['POST'])
def createUser():
    """
    Register User 
    """
    data = request.get_json()
    email = data.get('email','').strip().lower()
    pwd = data.get('password','')
    if not email or not pwd:
        return jsonify({'error':'email and passowrd required'}),400
    pw_hash = bcrypt.hashpw(password=pwd.encode(),salt=bcrypt.gensalt()).decode()
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('INSERT INTO USERS(email,password_hash) VALUES (%s,%s) RETURNING id',(email,pw_hash))
        user_id = cursor.fetchone()[0]
        conn.commit()
        return jsonify({'user_id':user_id,'email':email}),201
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        return jsonify({'error':'email already registered'}),409
    finally:
        conn.close()
    
@app.route("/login",methods = ['POST'])
def login():
    data = request.get_json()
    email = data.get("email",'').strip().lower()
    pwd = data.get("password",'')

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute('SELECT id,password_hash from users where email = %s',(email,))
        row = cur.fetchone()
    except Exception as e:
        return jsonify({"error":'something went wrong please try again'}),500
    finally:
        conn.close()
    if not row or not bcrypt.checkpw(pwd.encode(),row[1].encode()):
        return jsonify({'error':'invalid credentials'}),401
    token = jwt.encode({
        'user_id':row[0],
        'email':email,
        'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24)
        },SECRET,algorithm='HS256')
    return jsonify({'token':token})


@app.route("/health")
def health():
    return jsonify({
        "status":"ok",
        "service":"user_service"
    })


if __name__ == '__main__':
    app.run(host = "0.0.0.0",port = 8001)