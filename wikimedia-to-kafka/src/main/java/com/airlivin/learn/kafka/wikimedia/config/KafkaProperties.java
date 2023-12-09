package com.airlivin.learn.kafka.wikimedia.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class KafkaProperties extends Properties {
    public static final String KAFKA_PROPERTIES = System.getProperty("user.home") + "/.kafka/kafka.properties";

    public KafkaProperties() {
        try {
            load(new FileInputStream(KAFKA_PROPERTIES));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }
}
