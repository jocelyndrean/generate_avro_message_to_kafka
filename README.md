# Generate Avro Messages To Kafka

# Goals: 
This script Python generates fake data as Avro format to Kafka. This script was developped for this tutorial : 
- [FR] https://medium.com/@jocelyndrean/tuto-cr%C3%A9er-un-datapipeline-en-20-minutes-%C3%A0-laide-de-kafka-connect-841269f7bb55#.qcqy6gvd7

# Avro Schema:
```json
{
    "namespace": "io.confluent.connect.avro",
    "type": "record",
    "name": "test_event_record_avro",
    "fields": [
        { "name": "visitorId", "type": "int"},
        { "name": "pageViewId", "type": "int"},
        { "name": "timestamp", "type": "int"},
        { "name": "referer", "type": "string"},
        { "name": "pageType", "type": "string"},
        { "name": "userAgent", "type": "string"},
        { "name": "device", "type": "int"},
        { "name": "locale", "type": "string"},
        { "name": "eventType", "type":"string","default": "unknown"}
    ]
} 
```

# Configuration:
Please modify the file "generator.py", section Configuration
```python
SCHEMA_REGISTRY_URL = 'http://schema.registry.com:8081'
TOPIC = 'test_event_record_avro'
KAFKA_SERVER_URL = 'kafka.url.com:9092'
```

Install requirements
```bash
$ pip install -r requirements.txt
```
