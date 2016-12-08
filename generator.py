import StringIO
import struct
import requests
from avro import schema, io
import random
from faker import Faker
import time
import json
from kafka import SimpleProducer, KafkaClient
from kafka import TopicPartition

#Configuration
MAGIC_BYTE = 0
SCHEMA_REGISTRY_URL = 'http://schema.registry.com:8081'
TOPIC = 'test_event_record_avro'
KAFKA_SERVER_URL = 'kafka.url.com:9092'
#END Configuration

def find_latest_schema(topic):
    subject = topic + '-value'
    print "{}/subjects/{}/versions".format(SCHEMA_REGISTRY_URL, subject)

    versions_response = requests.get(
        url="{}/subjects/{}/versions".format(SCHEMA_REGISTRY_URL, subject),
        headers={
            "Content-Type": "application/vnd.schemaregistry.v1+json",
        },
    )

    latest_version = versions_response.json()[-1]

    schema_response = requests.get(
        url="{}/subjects/{}/versions/{}".format(SCHEMA_REGISTRY_URL, subject, latest_version),
        headers={
            "Content-Type": "application/vnd.schemaregistry.v1+json",
        },
    )
    schema_response_json = schema_response.json()

    print schema_response_json["id"]
    print schema.parse(schema_response_json["schema"])

    return schema_response_json["id"], schema.parse(schema_response_json["schema"])

#Encode a record as Avro
def encode_record(schema_id, schema, record):
    #construct avro writer
    writer = io.DatumWriter(schema)
    outf = StringIO.StringIO()

    # write the header
    # magic byte
    outf.write(struct.pack('b', MAGIC_BYTE))

    # write the schema ID in network byte order (big end)
    outf.write(struct.pack('>I', schema_id))

    # write the record to the rest of it
    # Create an encoder that we'll write to
    encoder = io.BinaryEncoder(outf)

    # write the magic byte
    # write the object in 'obj' as Avro
    writer.write(record, encoder)

    return outf.getvalue()

#Generate a record
def generate_record(fake):
    eventType = ['clic', 'addProductToCart', 'removeProductToCart', 'checkCart', 'search']
    referer = ['google', 'bing', 'yahoo', 'direct', 'partner', 'ads']

    message = {
        "visitorId": random.randint(1, 10000),
        "pageViewId": random.randint(1, 1000),
        "timestamp": int(time.time()),
        "referer": random.choice(referer),
        "pageType": fake.uri_page(),
        "userAgent": fake.user_agent(),
        "device": random.randint(1, 3), #1=desktop, 2=mobile, 3=tablet
        "locale": fake.locale(),
        "eventType": random.choice(eventType)
    }

    return message

if __name__ == '__main__':
    print "[init]"
    nb_error = 0
    nb_rows = 0
    print "Connection to Kafka..."
    kafka = KafkaClient(KAFKA_SERVER_URL)
    producer = SimpleProducer(kafka)  
    print "Get schema..."
    schema_id, schema = find_latest_schema(TOPIC)
    fake = Faker('fr_FR')
    
    print "Starting to generate Avro message to", TOPIC
    while True:
        try:
            if(nb_rows > 0 && nb_rows%1000 == 0):
                print "Rows generated =", nb_rows
            message = generate_record(fake)
            content = encode_record(schema_id, schema, message)
            producer.send_messages(TOPIC, content)
            nb_rows += 1
        except Exception, e:
            nb_error += 1
            print "Error detected"
            print str(e)
            print (nb_error)
        
