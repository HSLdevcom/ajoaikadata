import pulsar
import json

client = pulsar.Client("pulsar://localhost:6650")
consumer = client.subscribe("modified", subscription_name="my-sub")

while True:
    msg = consumer.receive()
    d = json.loads(msg.data())

    print("Time: ", d["eke_timestamp"])
    print("ID:", d["content"]["balise_id"])
    print("Next: ", d["content"]["balise_id_next"])
    # print("Dir", d["content"]["balise_cba"])
    print("Direction: ", d["content"].get("direction"))
    print()
    consumer.acknowledge(msg)

client.close()
