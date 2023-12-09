package com.airlivin.learn.kafka.wikimedia.sse;

import java.util.Map;

public record ServerSentEvent(String event, String id, String data) {

    public static ServerSentEvent of(Map<String, String> map) {
        return new ServerSentEvent(
                map.get("event"),
                map.get("id"),
                map.get("data"));
    }
}
