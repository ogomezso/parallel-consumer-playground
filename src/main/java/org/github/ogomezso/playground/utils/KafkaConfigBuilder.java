package org.github.ogomezso.playground.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaConfigBuilder {
    private final Properties kafkaProperties;

    public KafkaConfigBuilder(String propertiesName) throws IOException {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(propertiesName);
        this.kafkaProperties = new Properties();
        this.kafkaProperties.load(inputStream);
    }

    public KafkaConfigBuilder(String propertiesName, Boolean isPropertiesInClasspath) throws IOException {
        Object inputStream;
        if (isPropertiesInClasspath) {
            inputStream = this.getClass().getClassLoader().getResourceAsStream(propertiesName);
        } else {
            File propertiesFile = new File(propertiesName);
            inputStream = new FileInputStream(propertiesFile);
        }

        this.kafkaProperties = new Properties();
        this.kafkaProperties.load((InputStream)inputStream);
    }

    public Properties getKafkaProperties() {
        return this.kafkaProperties;
    }
}
