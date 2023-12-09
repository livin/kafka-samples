package com.airlivin.learn.kafka.elastic;

import com.airlivin.learn.kafka.config.KafkaProperties;
import com.airlivin.learn.kafka.elastic.consumer.WikimediaKafkaConsumer;
import com.airlivin.learn.kafka.elastic.producer.ElasticProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KafkaToElasticApp {
    private final WikimediaKafkaConsumer consumer;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(5);


    public KafkaToElasticApp() {
        KafkaProperties kafkaProps = new KafkaProperties();
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-to-elastic");
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ElasticProducer producer = new ElasticProducer();
        consumer = new WikimediaKafkaConsumer(kafkaProps);
    }


    public static void main(String[] args) {
        new KafkaToElasticApp().run();
    }

    private void run() {
        exitAfter();
        consumer.stream()
                .forEach(this::handleMessage);
    }

    private void handleMessage(String s) {
        System.out.println("Message: " + s);
    }

    private void exitAfter() {
        System.out.println("Scheduling finish in 10 secs.");
        executor.schedule(this::quit, 20, TimeUnit.SECONDS);
    }

    private void quit() {
        System.out.println("Exiting by timer");
        consumer.close();
        System.exit(0);
    }
}
