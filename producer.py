import json
import re
from kafka import KafkaProducer
from time import sleep

file_path = 'Sample1gb.json'
bootstrap_servers = ['localhost:9092']
topic = 'pp-topic'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def preprocess_value(value):
    if isinstance(value, str):
        # If value is a string, remove punctuations and extra spaces
        clean_value = re.sub(r'[^\w\s]', '', value)
        clean_value=clean_value.strip()
        return clean_value
    elif isinstance(value, list):
        # If value is a list, preprocess each element recursively
        return tuple([preprocess_value(v) for v in value])
    elif isinstance(value, dict):
        # If value is a dictionary (object), preprocess each value recursively
        return {k: preprocess_value(v) for k, v in value.items()}
    elif isinstance(value, str) and value.startswith("$"):
        # If value is a price string (starts with '$'), remove punctuations and convert to int
        clean_value = re.sub(r'[^\d.]', '', value)
        return int(float(clean_value))
    elif isinstance(value, str) and value.isdigit():
        # If value is an ASIN (digit string), convert to int
        return int(value)

    elif isinstance(value, str):
        return value
    else:
        # For other types, keep the value as is
        return value
    
def preprocess_dataset(chunk):
    preprocessed_chunk = []
    for row in chunk:
        preprocessed_row = {}
        for key, value in row.items():
            if key in ['category', 'title', 'also_buy', 'brand', 'rank', 'price', 'asin']:
                preprocessed_row[key] = preprocess_value(value)
        preprocessed_chunk.append(preprocessed_row)
    return preprocessed_chunk

with open(file_path, 'r') as file:
    for line in file:
        json_data = json.loads(line)
        preprocessed_data = preprocess_dataset([json_data])
        producer.send(topic, value=preprocessed_data[0])
        print('Data sent to Kafka:', preprocessed_data[0])
        sleep(2)  # Adjust sleep time as needed