package com.airlivin.learn.kafka.elastic.consumer;

import com.airlivin.learn.kafka.config.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Stream;

public class WikimediaKafkaConsumer {
    private final static Logger log = LoggerFactory.getLogger(WikimediaKafkaConsumer.class);
    private static final String WIKIMEDIA_TOPIC = "wikimedia";
    private final KafkaConsumer<String, String> consumer;
    private final Executor executor = Executors.newSingleThreadExecutor();

    private final LinkedBlockingDeque<List<String>> queue = new LinkedBlockingDeque<>();
    private boolean alive;
    private int messagesReceived = 0;

    public WikimediaKafkaConsumer(KafkaProperties kafkaProps) {
        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumer = new KafkaConsumer<>(kafkaProps);
    }

    public Stream<List<String>> stream() {
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
                addBatch(consumer.poll(Duration.ofSeconds(2)));
                consumer.commitSync();
            } catch (WakeupException wake) {
                alive = false;
            }
        }
        log.info("Received messages: " + messagesReceived);
    }

    private void addBatch(ConsumerRecords<String, String> records) {
        if (records.count() == 0)
            return; // not a batch

        log.info("Received batch: " + records.count());

        queue.addLast(toStringRecords(records));

        messagesReceived += records.count();
    }

    private static List<String> toStringRecords(ConsumerRecords<String, String> records) {
        var list = new ArrayList<ConsumerRecord<String, String>>();
        records.forEach(list::add);
        return list.stream().map(ConsumerRecord::value).toList();
    }

    private List<String> takeItem() {
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
