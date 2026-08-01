package com.demo.flink.pipeline;

import com.demo.flink.common.JsonUtil;
import com.demo.flink.model.Position;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

/** Keyed by account|ticker so each position key lands on a stable partition (upsert semantics). */
public class PositionKafkaSerializer implements KafkaRecordSerializationSchema<Position> {

    private final String topic;

    public PositionKafkaSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Position position, KafkaSinkContext context, Long timestamp) {
        byte[] key = (position.account + "|" + position.ticker).getBytes(StandardCharsets.UTF_8);
        byte[] value = JsonUtil.toJson(position).getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(topic, key, value);
    }
}
