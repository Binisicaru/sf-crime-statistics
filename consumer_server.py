from kafka import KafkaConsumer

BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "sf_crime"
GROUP_ID="Integration-KS-01"
AOR = "earliest"

def run_consumer_server():
    consumer = KafkaConsumer(
        TOPIC,
        group_id = GROUP_ID,
        bootstrap_servers = BOOTSTRAP_SERVER,
        auto_offset_reset= AOR
    )
    
    for msg in consumer:
        if msg is not None:
            print(msg)
            
if __name__ == "__main__":
    run_consumer_server()