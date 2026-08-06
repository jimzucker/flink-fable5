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
    // Fields are matched independently: JSON key order is not guaranteed (the
    // SQL pipeline's JSON_OBJECT emits a different order than the DataStream
    // serializer), and numeric fields may or may not be quoted. An ordered,
    // quote-required pattern silently matched nothing and reported "checked=0",
    // which reads like success rather than "the check never ran".
    private static final Pattern QTY = Pattern.compile("\"net_qty\"\\s*:\\s*\"?(-?\\d+)\"?");
    private static final Pattern PRICE = Pattern.compile("\"price\"\\s*:\\s*\"?(-?[0-9.]+)\"?");
    private static final Pattern MVAL = Pattern.compile("\"mv\"\\s*:\\s*\"?(-?[0-9.]+)\"?");

    private static String mathSample = null;
    private static long mathChecked = 0;
    private static long mathBad = 0;

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
            // Assign partitions directly rather than subscribe(): a consumer
            // GROUP needs AlterGroup/DescribeGroup, and when that authorization
            // fails poll() just returns empty forever instead of throwing — a
            // silent failure that reads exactly like "the pipeline produced
            // nothing". Manual assignment needs only ReadData on the topic.
            List<org.apache.kafka.common.TopicPartition> parts = new ArrayList<>();
            for (org.apache.kafka.common.PartitionInfo pi : consumer.partitionsFor(topic)) {
                parts.add(new org.apache.kafka.common.TopicPartition(topic, pi.partition()));
            }
            System.out.println("latency-probe: assigned " + parts.size() + " partitions of " + topic);
            consumer.assign(parts);
            consumer.seekToEnd(parts);
            while (System.currentTimeMillis() < end) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value() == null) {
                        continue; // upsert tombstone
                    }
                    Matcher q = QTY.matcher(record.value());
                    Matcher p = PRICE.matcher(record.value());
                    Matcher v = MVAL.matcher(record.value());
                    if (q.find() && p.find() && v.find()) {
                        mathChecked++;
                        java.math.BigDecimal expect = new java.math.BigDecimal(p.group(1))
                                .multiply(new java.math.BigDecimal(q.group(1)));
                        if (expect.compareTo(new java.math.BigDecimal(v.group(1))) != 0) {
                            mathBad++;
                            if (mathBad <= 3) {
                                System.out.println("math-verify MISMATCH: " + record.value());
                            }
                        }
                    } else if (mathSample == null) {
                        // Keep one unmatched record so a zero count is diagnosable
                        // instead of looking like a clean pass.
                        mathSample = record.value();
                    }
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
        if (params.getBoolean("probe.verify.math", false)) {
            System.out.printf("math-verify: topic=%s checked=%d mismatches=%d%n",
                    topic, mathChecked, mathBad);
            if (mathChecked == 0 && mathSample != null) {
                System.out.println("math-verify: NO FIELDS MATCHED — sample record: " + mathSample);
            }
        }
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
