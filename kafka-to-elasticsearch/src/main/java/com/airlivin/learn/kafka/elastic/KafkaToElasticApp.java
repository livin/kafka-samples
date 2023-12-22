package com.airlivin.learn.kafka.elastic;

import com.airlivin.learn.kafka.config.KafkaProperties;
import com.airlivin.learn.kafka.elastic.consumer.WikimediaKafkaConsumer;
import com.airlivin.learn.kafka.elastic.producer.ElasticProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.System.out;

public class KafkaToElasticApp {
    private static final Logger log = LoggerFactory.getLogger(KafkaToElasticApp.class);
    private final WikimediaKafkaConsumer consumer;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    private final ElasticProducer producer;

    public KafkaToElasticApp() {
        KafkaProperties kafkaProps = new KafkaProperties();
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-to-elastic");
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try {
            kafkaProps.load(new FileInputStream("user.properties"));
        } catch (IOException e) {
            log.error("No user properties file found. Please put user.properties with elastic settings.");
        }

        producer = new ElasticProducer(kafkaProps);
        consumer = new WikimediaKafkaConsumer(kafkaProps);
    }

    public static void main(String[] args) {
        new KafkaToElasticApp().run();
    }

    private void run() {
        exitAfter(30);
        producer.stream(consumer.stream());
    }

    private void exitAfter(final int seconds) {
        out.println(STR."Scheduling finish in \{seconds} secs.");
        executor.schedule(this::quit, seconds, TimeUnit.SECONDS);
    }

    private void quit() {
        out.println("Exiting by timer");
        consumer.close();
        System.exit(0);
    }
}
