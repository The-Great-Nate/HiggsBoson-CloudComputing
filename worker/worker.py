import json
import pika
import time
import os
import awkward as ak
import numpy as np
import vector
import uproot
import HZZAnalysis_Funcs as HZZ


data_dir = "/data/"

def OnMessage(channel, method, properties, body):
    try:
        task = json.loads(body)
        sample = task["sample"]
        file_path = task["file"]
        full_path = os.path.join(data_dir, file_path)

        print(f"[worker] Processing file: {full_path} from sample: {sample}")
        
        sample_data = HZZ.process_data(file_path, sample)
        
        out_file = os.path.join(
            data_dir,
            f"{sample}-{os.path.basename(file_path)}_frames.parquet"
        )
        ak.to_parquet(sample_data, out_file)
        

        print(f"Finished processing file: {full_path}")
        
        channel.basic_publish(
            exchange='',
            routing_key='aggregate v3',
            body=json.dumps({"file_done": True})
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error processing file {full_path}: {e}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)




connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq')
)
channel = connection.channel()
channel.queue_declare(queue='tasks v3', durable=True)
channel.queue_declare(queue='aggregate v3', durable=True)
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='tasks v3', on_message_callback=OnMessage)
print("Worker ready, waiting for tasks...")
try:
    channel.start_consuming()
except Exception as e:
    print(f"Error during consuming: {e}")
finally:
    channel.stop_consuming()
    connection.close()


