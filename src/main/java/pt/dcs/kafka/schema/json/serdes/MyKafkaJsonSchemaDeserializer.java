package pt.dcs.kafka.schema.json.serdes;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static pt.dcs.kafka.schema.SerDesConfig.*;

public class MyKafkaJsonSchemaDeserializer<T> extends KafkaJsonSchemaDeserializer<T> {

    private Boolean isKey;

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
        this.isKey = isKey;
        super.configure(config, isKey);
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] bytes) {
        var newBytes = bytes;
        var header = headers.lastHeader(isKey ? DEFAULT_KEY_SCHEMA_VERSION_ID : DEFAULT_VALUE_SCHEMA_VERSION_ID);
        if (header != null) {
            // The following is a trick to add back Confluent message wire format:
            // AbstractKafkaSchemaSerDe.MAGIC_BYTE + AbstractKafkaSchemaSerDe.idSize
            // The goal is to have the record as Confluent deserializer expects it to be
            try (var out = new ByteArrayOutputStream()) {
                out.write(MAGIC_BYTE);
                out.write(header.value());
                out.write(bytes);
                newBytes = out.toByteArray();
            } catch (IOException e) {
                throw new SerializationException("Something went wrong while adding schema id to the record", e);
            }
        }
        return super.deserialize(topic, headers, newBytes);
    }

}