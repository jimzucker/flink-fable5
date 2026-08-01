package com.demo.flink.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * All runtime knobs come from a properties file (--config /path/to/application.properties),
 * overridable by --key value command-line args — behavior changes never require a rebuild.
 * Deliberately dependency-free so it works in the generator's plain-JRE container too.
 */
public final class AppConfig implements Serializable {

    private final Properties props = new Properties();

    private AppConfig() {
    }

    public static AppConfig load(String[] args) throws IOException {
        AppConfig config = new AppConfig();
        // Base layer: Managed Service for Apache Flink runtime properties (no-op elsewhere)
        try {
            java.util.Map<String, Properties> groups =
                    com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime.getApplicationProperties();
            Properties msf = groups == null ? null : groups.get("FlinkApplicationProperties");
            if (msf != null) {
                config.props.putAll(msf);
            }
        } catch (Throwable ignored) {
            // not running on MSF — file/args below are the source of truth
        }
        // Next: locate --config and load the file
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("--config")) {
                try (InputStream in = new FileInputStream(args[i + 1])) {
                    config.props.load(in);
                }
            }
        }
        // Second pass: --key value args override file values
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].startsWith("--") && !args[i].equals("--config") && !args[i + 1].startsWith("--")) {
                config.props.setProperty(args[i].substring(2), args[i + 1]);
            }
        }
        return config;
    }

    public boolean has(String key) {
        return props.containsKey(key);
    }

    public String get(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        return has(key) ? Integer.parseInt(props.getProperty(key).trim()) : defaultValue;
    }

    public long getLong(String key, long defaultValue) {
        return has(key) ? Long.parseLong(props.getProperty(key).trim()) : defaultValue;
    }

    public double getDouble(String key, double defaultValue) {
        return has(key) ? Double.parseDouble(props.getProperty(key).trim()) : defaultValue;
    }

    /**
     * Passthrough Kafka client properties: every `kafka.props.<x>=<v>` config entry
     * becomes `<x>=<v>` on the consumer/producer — how MSK IAM auth (SASL_SSL etc.)
     * is enabled on AWS with zero code changes.
     */
    public Properties kafkaProps() {
        Properties kafka = new Properties();
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith("kafka.props.")) {
                kafka.setProperty(name.substring("kafka.props.".length()), props.getProperty(name));
            }
        }
        return kafka;
    }
}
