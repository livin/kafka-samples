package com.airlivin.learn.kafka.wikimedia.producer;

import com.airlivin.learn.kafka.config.KafkaProperties;
import com.airlivin.learn.kafka.wikimedia.sse.ServerSentEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.stream.Stream;

public class SSEEventProducer {
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
        producer = new KafkaProducer<>(kafkaProps);
    }

    /**
     * Streams source SSEEvents to kafka topic.
     * Takes event.data as value for kafka message.
     *
     * @param sourceStream an input stream of SSEEvents
     */
    public void stream(Stream<ServerSentEvent> sourceStream) {
        sourceStream.forEach(this::send);
    }

    private void send(ServerSentEvent event) {
        producer.send(new ProducerRecord<>(topic, event.data()));
        System.out.println("Sent: " + event);
    }
}
