import producer_server
from pathlib import Path

def run_kafka_server():
	# TODO get the json file path
    input_file = "police-department-calls-for-service.json" #f"{Path().cwd()}/police-department-calls-for-service.json"
    print("The input file is: " + input_file)
    BOOTSTRAP_SERVER = "localhost:9092"
    
    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="sf_crime",
        bootstrap_servers=BOOTSTRAP_SERVER,
        client_id="sf_crime-client"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()

if __name__ == "__main__":
    feed()
