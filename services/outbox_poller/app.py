import psycopg2
import os
import time
from confluent_kafka import Producer
import json

print("Bootstrap Server",os.getenv('KAFKA_BOOTSTRAP'))
p = Producer({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP')})

DB_URL = os.getenv("DATABASE_URL")


def get_connection():
    return psycopg2.connect(DB_URL)

connection = get_connection()
connection.autocommit = False
results = {}
def on_delivery(err, msg):
    row_id = int(msg.key())
    results[row_id] = err 
    print(f"result for {row_id} is {err}")
while True:
    results.clear()

    try:
        print("poller is running")
        cur = connection.cursor()
        cur.execute(
            "SELECT id,topic,payload from outbox where published_at is null and attempts<5 ORDER BY id ASC LIMIT 100 FOR UPDATE SKIP LOCKED "
        )
        rows = cur.fetchall()
        print("haha",rows)
        for row_id, topic, payload in rows:
            print("pushing to kafka for this ",row_id, topic, payload)
            p.produce(topic, json.dumps(payload),key=str(row_id),on_delivery=on_delivery)

        p.flush()
        for row_id, err in results.items():
            if err is None:
                cur.execute('UPDATE outbox SET published_at=NOW() WHERE id=%s', (row_id,))
            else:
                cur.execute('UPDATE outbox SET attempts=attempts+1 WHERE id=%s', (row_id,))
        connection.commit()
        
        time.sleep(5)

    except Exception as e:
        print(f"something went wrong please try again {str(e)}")
        try:
            connection.close()
        except:
            pass
        connection = get_connection()
        time.sleep(5)








