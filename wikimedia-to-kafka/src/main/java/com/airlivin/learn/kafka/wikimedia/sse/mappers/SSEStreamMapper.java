package com.airlivin.learn.kafka.wikimedia.sse.mappers;

import com.airlivin.learn.kafka.wikimedia.sse.ServerSentEvent;
import com.airlivin.learn.kafka.wikimedia.sse.parser.SSEEventParser;
import reactor.core.publisher.Flux;

import java.util.stream.Stream;

public class SSEStreamMapper {
    private final SSEEventParser parser = new SSEEventParser();

    /**
     * Takes live stream of lines of SSEs
     * and streams SSEs.
     *
     * @param body a stream of string lines from server with SSEs.
     * @return live stream of SSEs.
     */
    public Stream<ServerSentEvent> toEvents(Stream<String> body) {
        return Flux.fromStream(body)
                .bufferUntil(String::isEmpty)
                .map(parser::parse)
                .toStream(1);
    }
}
