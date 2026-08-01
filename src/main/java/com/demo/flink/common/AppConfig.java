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
        // First pass: locate --config and load the file
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
}
