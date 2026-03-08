from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from flask import Flask,jsonify
import os
import json
import requests
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")




admin = AdminClient({'bootstrap.servers':KAFKA_BOOTSTRAP})
c= Consumer(
    {
        'bootstrap.servers' : KAFKA_BOOTSTRAP,
        'group.id': 'admin-service'
    }
)


app = Flask(__name__)

@app.route("/admin/kafka/lag")
def kafka_lag():
    metadata = admin.list_topics(timeout=10)    
    result = []
    for topic_name, topic_metadata in metadata.topics.items():
        if topic_name.startswith('__'):
            continue  # skip internal topics
        
        for partition_id in topic_metadata.partitions.keys():
            tp = TopicPartition(topic_name, partition_id)
            low,high  = c.get_watermark_offsets(tp)
            committed  = c.committed([tp])
            committed_offset = committed[0].offset
            if committed_offset < 0:  # -1001 means no committed offset
                committed_offset = 0
            result.append({'topic_name':topic_name,"lag":(high-committed_offset)})
        # get committed offset and end offset

    return jsonify(result),200

@app.route("/admin/health")
def health():
    services = {
        'user_service': 'http://user_service:8001/health',
        'upload_service': 'http://upload_service:8002/health',
        'query_service': 'http://query_service:8003/health',
    }    
    results = {}
    for name,service_endpoint in services.items():
        try:
            resp = requests.get(service_endpoint,timeout=3)
            results[name] = 'ok' if resp.status_code == 200 else 'degraded'
        except:
            results[name] = "down"

    # get committed offset and end offset

    return jsonify(results),200

if __name__ == '__main__':
    app.run(host="0.0.0.0",port=8004)