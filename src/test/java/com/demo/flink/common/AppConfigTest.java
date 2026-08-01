package com.demo.flink.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class AppConfigTest {

    @Test
    void fileValuesLoadedAndArgsOverride(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("app.properties");
        Files.writeString(file, "generator.trades.per.sec=10\npipeline.parallelism=2\n");

        AppConfig config = AppConfig.load(new String[]{
                "--config", file.toString(),
                "--generator.trades.per.sec", "1000"});

        assertEquals(1000, config.getInt("generator.trades.per.sec", -1), "arg overrides file");
        assertEquals(2, config.getInt("pipeline.parallelism", -1), "file value kept");
        assertEquals(7, config.getInt("missing.key", 7), "default when absent");
        assertFalse(config.has("missing.key"));
    }
}
