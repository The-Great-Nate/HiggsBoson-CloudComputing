import json
import pika
import time
from atlasopenmagic import install_from_environment
install_from_environment()

import atlasopenmagic as atom

def get_file_amount(samples):
    total_files = 0
    for s in samples:
        total_files += len(samples[s]['list'])
    return total_files

def main():
    atom.available_releases()
    atom.set_release('2025e-13tev-beta')
    
    skim = "exactly4lep"
    defs = {
        r'Data':{'dids':['data']},
        r'Background $Z,t\bar{t},t\bar{t}+V,VVV$':{'dids': [410470,410155,410218,
                                                            410219,412043,364243,
                                                            364242,364246,364248,
                                                            700320,700321,700322,
                                                            700323,700324,700325], 'color': "#6b59d3" }, # purple
        r'Background $ZZ^{*}$':     {'dids': [700600],'color': "#ff0000" },# red
        r'Signal ($m_H$ = 125 GeV)':  {'dids': [345060, 346228, 346310, 346311, 346312,
                                            346340, 346341, 346342],'color': "#00cdff" },# light blue
}
    samples = atom.build_dataset(defs, skim=skim, protocol='https', cache=True)

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq')
    )
    channel = connection.channel()
    channel.queue_declare(queue='tasks v3', durable = True)
    channel.queue_declare(queue='aggregate v3', durable = True)

    done_msg = {"file_count": str(get_file_amount(samples))}
    print("PRODUCER: Sending total file count:", done_msg["file_count"])
    channel.basic_publish(
        exchange="",
        routing_key='aggregate v3',
        body=json.dumps(done_msg)
    )
    
    meta = {name: {"color": defs[name].get("color", "#000000")} for name in defs.keys()}
    print("PRODUCER: Senting sample metadata to aggregator")
    channel.basic_publish(
        exchange='',
        routing_key='aggregate v3',
        body=json.dumps({"metadata": meta})
    )

    start_time = time.time()
    for s in samples:
        for val in samples[s]['list']:
            task = {"sample": s, "file": val}
            channel.basic_publish(
                exchange='',
                routing_key='tasks v3',
                body=json.dumps(task),
                properties=pika.BasicProperties(
                         delivery_mode = pika.DeliveryMode.Persistent #Make message persistent
                )   
            )
            print("Queued:", task)
        
    elapsed_time = time.time() - start_time
    
    with open("/data/producer_perf.json", "w") as f:
        json.dump({"task_alloc_time": elapsed_time}, f)

    connection.close()

if __name__ == "__main__":
    main()