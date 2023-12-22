package com.airlivin.learn.kafka.wikimedia.producer;

import com.airlivin.learn.kafka.config.KafkaProperties;
import com.airlivin.learn.kafka.wikimedia.sse.ServerSentEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

public class SSEEventProducer {
    private static final Logger log = LoggerFactory.getLogger(SSEEventProducer.class);
    private final KafkaProducer<String, String> producer;
    private final String topic;

    /**
     * Creates producer for given topic
     * that takes SSEEvents as input.
     * @param topic target kafka topic.
     */
    public SSEEventProducer(String topic) {
        this.topic = topic;
        KafkaProperties kafkaProps = new KafkaProperties();
        kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");
        kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        producer = new KafkaProducer<>(kafkaProps);
    }

    /**
     * Streams source SSEEvents to kafka topic.
     * Takes event.data as value for kafka message.
     *
     * @param sourceStream an input stream of SSEEvents
     */
    public void stream(Stream<ServerSentEvent> sourceStream) {
        log.info("Started streaming messages...");
        sourceStream.forEach(this::send);
    }

    private void send(ServerSentEvent event) {
        producer.send(new ProducerRecord<>(topic, event.data()));

    }
}
