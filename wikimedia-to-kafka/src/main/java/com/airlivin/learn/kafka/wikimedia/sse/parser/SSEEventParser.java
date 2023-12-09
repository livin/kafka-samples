package com.airlivin.learn.kafka.wikimedia.sse.parser;

import com.airlivin.learn.kafka.wikimedia.sse.ServerSentEvent;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toMap;

public class SSEEventParser {

    public ServerSentEvent parse(List<String> lines) {
        Map<String, String> map = lines.stream()
                .map(this::parseLine)
                .filter(Objects::nonNull)
                .collect(
                        toMap(
                                AbstractMap.SimpleEntry::getKey,
                                AbstractMap.SimpleEntry::getValue));
        return ServerSentEvent.of(map);
    }

    private AbstractMap.SimpleEntry<String, String> parseLine(String line) {
        if (line == null || line.isEmpty())
            return null;

        int colonIndex = line.indexOf(':');
        if (colonIndex > 0) {
            return parseKeyValue(line, colonIndex);
        }
        return null;
    }

    private AbstractMap.SimpleEntry<String, String> parseKeyValue(String line, int colonIndex) {
        String key = line.substring(0, colonIndex);
        String value = line.substring(colonIndex + 1).stripLeading();
        return new AbstractMap.SimpleEntry<>(key, value);
    }
}
