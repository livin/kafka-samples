package com.airlivin.learn.kafka.elastic.consumer;

import com.airlivin.learn.kafka.config.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Stream;

public class WikimediaKafkaConsumer {
    private static final String WIKIMEDIA_TOPIC = "wikimedia";
    private final KafkaConsumer<String, String> consumer;
    private final Executor executor = Executors.newSingleThreadExecutor();

    private final LinkedBlockingDeque<String> queue = new LinkedBlockingDeque<>();
    private boolean alive;
    private int messagesReceived = 0;

    public WikimediaKafkaConsumer(KafkaProperties kafkaProps) {
        consumer = new KafkaConsumer<>(kafkaProps);
    }

    public Stream<String> stream() {
        listen();
        return Stream.generate(this::takeItem);
    }

    private void listen() {
        executor.execute(this::readLoop);
    }

    private void readLoop() {
        consumer.subscribe(List.of(WIKIMEDIA_TOPIC));
        alive = true;
        while (alive) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                records.forEach(this::addItem);
            } catch (WakeupException wake) {
                alive = false;
            }
        }
        System.out.println("Received messages: " + messagesReceived);
    }

    private void addItem(ConsumerRecord<String, String> r) {
        if (r.value() == null)
            return;

        queue.addLast(r.value());
        messagesReceived++;
    }

    private String takeItem() {
        try {
            return queue.takeFirst();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        alive = false;
        consumer.wakeup();
    }
}
