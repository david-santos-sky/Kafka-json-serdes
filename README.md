# Kafka-json-serdes
`MyKafkaJsonSchemaSerializer` and `MyKafkaJsonSchemaDeserializer`, based on Confluent `KafkaJsonSchemaSerializer` and `KafkaJsonSchemaDeserializer` (respectively), but without using Confluent wire format and using Kafka headers instead to store the schema ID.
