package com.airlivin.learn.kafka.wikimedia;

import com.airlivin.learn.kafka.wikimedia.consumer.WikiMediaSSEConsumer;
import com.airlivin.learn.kafka.wikimedia.producer.SSEEventProducer;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * App wires kafka producer with Wikimedia SSE events consumer
 * and sends events into "wikimedia" topic.
 */
public class WikimediaProducerApp {

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    public static void main(String[] args) throws IOException, InterruptedException {
        new WikimediaProducerApp().run();
    }

    private void run() throws IOException, InterruptedException {
        exitAfter();

        new SSEEventProducer("wikimedia")
                .stream(new WikiMediaSSEConsumer().stream());
    }

    private void exitAfter() {
        executor.schedule(() -> System.exit(0), 60, SECONDS);
    }
}
