package com.airlivin.learn.kafka.wikimedia.consumer;

import com.airlivin.learn.kafka.wikimedia.sse.ServerSentEvent;
import com.airlivin.learn.kafka.wikimedia.sse.mappers.SSEStreamMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.stream.Stream;

public class WikiMediaSSEConsumer {
    private final URI uri = URI.create("https://stream.wikimedia.org/v2/stream/recentchange");
    private final HttpClient client = HttpClient.newHttpClient();
    private final HttpRequest request = HttpRequest.newBuilder(uri).GET().build();
    private final SSEStreamMapper mapper = new SSEStreamMapper();

    /**
     * Produces stream of SSEEvents from Wikimedia site.
     *
     * @return live stream of events
     * @throws IOException when there is issue with IO.
     * @throws InterruptedException when execution was interrupted
     */
    public Stream<ServerSentEvent> stream() throws IOException, InterruptedException {
        return mapper.toEvents(
                client.send(
                        request,
                        HttpResponse.BodyHandlers.ofLines()).body());
    }
}
