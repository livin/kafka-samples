package com.airlivin.learn.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Kafka properties storage which
 * loads properties from user and sets some defaults.
 */
public class KafkaProperties extends Properties {
    public KafkaProperties() {
        setStringSerializers();

        loadUserProperties();
    }

    protected void setStringSerializers() {
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }

    protected String getUserPropertiesFile() {
        return System.getProperty("user.home") + "/.kafka/kafka.properties";
    }

    /**
     * Loads properties from user home dir.
     */
    protected void loadUserProperties() {
        try {
            load(new FileInputStream(getUserPropertiesFile()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
