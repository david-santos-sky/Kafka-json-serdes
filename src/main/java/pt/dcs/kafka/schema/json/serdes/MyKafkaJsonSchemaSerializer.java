package pt.dcs.kafka.schema.json.serdes;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.common.header.Headers;

import java.util.Arrays;
import java.util.Map;

import static pt.dcs.kafka.schema.SerDesConfig.*;

public class MyKafkaJsonSchemaSerializer<T> extends KafkaJsonSchemaSerializer<T> {

    private boolean isKey;
    private boolean useRecordHeader;

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
        this.isKey = isKey;
        this.useRecordHeader = getOrDefaultAsBoolean(config, USE_SCHEMA_VERSION_ID_IN_HEADER, true);
        super.configure(config, isKey);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T record) {
        var payload = super.serialize(topic, headers, record);
        if (this.useRecordHeader) {
            // The following is a trick to remove Confluent message wire format:
            // AbstractKafkaSchemaSerDe.MAGIC_BYTE + AbstractKafkaSchemaSerDe.idSize
            // The goal is to remove the schema id information from the record and put it a header,
            // thus leaving the record itself untouched
            var extraBytesSize = 1 + idSize;
            var id = Arrays.copyOfRange(payload, 1, extraBytesSize);
            headers.add(this.isKey ? DEFAULT_KEY_SCHEMA_VERSION_ID : DEFAULT_VALUE_SCHEMA_VERSION_ID, id);
            return Arrays.copyOfRange(payload, extraBytesSize, payload.length);
        }
        return payload;
    }

}