import json
import pika
import time
import os
import awkward as ak
import numpy as np
import vector
import uproot
import HZZAnalysis_Funcs as HZZ


data_dir = "/data/" #Establish directory to store data

def OnMessage(channel, method, properties, body):
    '''
    Arguments:
        channel (pika.BlockingConnection.channel) = a rabbitmq object
        method = needed
        properties = utilised in template. will keep for safety
        body = Recieved message
    Description:
        This funtion recieves a URL dictionary from the tasks queue and passes it into the
        data analysis function adapted from the original notebook.
    '''
    try: # Reading url of file
        task = json.loads(body)
        sample = task["sample"]
        file_path = task["file"]
        full_path = os.path.join(data_dir, file_path)

        print(f"[worker] Processing file: {full_path} from sample: {sample}")
        
        sample_data = HZZ.process_data(file_path, sample) # Perform the analysis
        
        out_file = os.path.join(
            data_dir,
            f"{sample}-{os.path.basename(file_path)}_frames.parquet"
        )
        ak.to_parquet(sample_data, out_file)
        

        print(f"Finished processing file: {full_path}")
        
        ######## Tell aggregator "I have processed a file!!!" ########
        channel.basic_publish(
            exchange='',
            routing_key='aggregate v3',
            body=json.dumps({"file_done": True})
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e: #If there is an error, requeue the task
        print(f"Error processing file {full_path}: {e}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)



######## Declaring rabbitmq queues ########
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq')
)
channel = connection.channel()
channel.queue_declare(queue='tasks v3', durable=True)
channel.queue_declare(queue='aggregate v3', durable=True)
channel.basic_qos(prefetch_count=1)

######## Start consuming files to process (nom nom nom) ########
channel.basic_consume(queue='tasks v3', on_message_callback=OnMessage)
print("Worker ready, waiting for tasks...")

try:
    channel.start_consuming()
except Exception as e:
    print(f"Error during consuming: {e}")
finally:
    channel.stop_consuming()
    connection.close()


