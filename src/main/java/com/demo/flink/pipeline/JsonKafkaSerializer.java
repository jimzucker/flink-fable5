package com.demo.flink.pipeline;

import com.demo.flink.common.JsonUtil;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * JSON sink serializer for any model. Records are keyed (upsert stream, stable
 * partition per key) and output volume is measured where the real bytes exist:
 * records out, bytes out, KB/sec meter per sink.
 */
public class JsonKafkaSerializer<T> implements KafkaRecordSerializationSchema<T> {

    public interface KeyFn<T> extends Function<T, String>, Serializable {
    }

    private final String topic;
    private final KeyFn<T> keyFn;
    private transient Counter recordsOut;
    private transient Counter bytesOut;

    public JsonKafkaSerializer(String topic, KeyFn<T> keyFn) {
        this.topic = topic;
        this.keyFn = keyFn;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) {
        MetricGroup group = context.getMetricGroup();
        recordsOut = group.counter("demoRecordsOut");
        bytesOut = group.counter("demoBytesOut");
        group.meter("demoBytesOutPerSecond", new MeterView(bytesOut));
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T element, KafkaSinkContext context, Long timestamp) {
        byte[] key = keyFn.apply(element).getBytes(StandardCharsets.UTF_8);
        byte[] value = JsonUtil.toJson(element).getBytes(StandardCharsets.UTF_8);
        recordsOut.inc();
        bytesOut.inc(key.length + value.length);
        return new ProducerRecord<>(topic, key, value);
    }
}
