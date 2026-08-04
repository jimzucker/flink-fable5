package com.demo.flink.generator;

import com.demo.flink.common.AppConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * End-to-end latency probe, identical on every cloud: subscribes to an
 * output topic at the head, and for each record measures
 * (Kafka record timestamp at the sink) - (event_time/as_of in the payload)
 * = full pipeline latency from source event to sink write. Prints
 * count/p50/p95/p99/max after the run. Plain JRE, same jar as the
 * generator, so it runs on a laptop against Confluent or as an ECS
 * run-task inside the VPC against MSK.
 */
public class LatencyProbe {

    private static final Pattern FIELD = Pattern.compile("\"(as_of|event_time)\"\\s*:\\s*(\\d+)");

    public static void main(String[] args) throws Exception {
        AppConfig params = AppConfig.load(args);
        String bootstrap = params.get("kafka.bootstrap.servers", "localhost:29092");
        String topic = params.get("probe.topic", "mv-by-account-ticker");
        long durationSec = params.getLong("probe.duration.sec", 120L);

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "latency-probe-" + UUID.randomUUID());
        props.put("auto.offset.reset", "latest");
        props.putAll(params.kafkaProps());

        List<Long> latencies = new ArrayList<>();
        long end = System.currentTimeMillis() + durationSec * 1000;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            while (System.currentTimeMillis() < end) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    Matcher m = FIELD.matcher(record.value());
                    if (m.find()) {
                        long eventTime = Long.parseLong(m.group(2));
                        long latency = record.timestamp() - eventTime;
                        if (latency >= 0 && latency < 600_000) { // ignore replays/clock junk
                            latencies.add(latency);
                        }
                    }
                }
            }
        }
        Collections.sort(latencies);
        if (latencies.isEmpty()) {
            System.out.println("latency-probe: topic=" + topic + " NO RECORDS OBSERVED");
            return;
        }
        System.out.printf("latency-probe: topic=%s n=%d p50=%dms p95=%dms p99=%dms max=%dms%n",
                topic, latencies.size(),
                pct(latencies, 0.50), pct(latencies, 0.95), pct(latencies, 0.99),
                latencies.get(latencies.size() - 1));
    }

    private static long pct(List<Long> sorted, double q) {
        return sorted.get(Math.min(sorted.size() - 1, (int) Math.floor(q * sorted.size())));
    }
}
