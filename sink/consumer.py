import pulsar
import json
client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe('modified', subscription_name='my-sub')

while True:
    msg = consumer.receive()
    # print(f"{msg.partition_key()}: {json.loads(msg.data())}")
    d = json.loads(msg.data())

    print("Time: ", d["eke_timestamp"])         
    print("ID:", d["content"]["content"]["balise_id"])
    print("Next: ", d["content"]["content"]["balise_id_next"])
    print("Dir", d["content"]["content"]["balise_cba"])
    print()
    consumer.acknowledge(msg)

client.close()