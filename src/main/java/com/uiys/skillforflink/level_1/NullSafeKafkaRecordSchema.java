package com.uiys.skillforflink.level_1;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

/**
 * @author uiys
 */
public class NullSafeKafkaRecordSchema implements KafkaRecordDeserializationSchema<String> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out) {
        if (record.value() == null) {
            return;
        }
        String value = new String(record.value(), StandardCharsets.UTF_8);
        out.collect(value);
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }


}