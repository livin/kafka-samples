package com.airlivin.learn.kafka.elastic.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

public class ElasticProducer {
    private static final Logger log = LoggerFactory.getLogger(ElasticProducer.class);
    public static final String WIKIMEDIA_INDEX = "wikimedia";

    private final Properties props;

    private final RestHighLevelClient client;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public ElasticProducer(Properties props) {
        this.props = props;
        client = new RestHighLevelClient(createRestClient());
    }

    private String getServerUrl() {
        return props.getProperty("elastic.serverUrl", "https://localhost:9200");
    }

    private String getUsername() {
        return props.getProperty("elastic.username");
    }
    private String getPassword() {
        return props.getProperty("elastic.password");
    }

    public void stream(Stream<List<String>> stream) {
        stream.forEach(this::indexBatch);
    }

    private void indexBatch(List<String> batch) {
        try {
            BulkRequest bulkRequest = new BulkRequest(WIKIMEDIA_INDEX);
            batch.stream()
                    .filter(Objects::nonNull)
                    .forEach(json -> bulkRequest.add(createIndexRequest(json)));

            BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);

            log.info(STR."Bulk index status: \{response.status()}. Total records: \{batch.size()}");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private IndexRequest createIndexRequest(String json) {
        IndexRequest request = new IndexRequest();
        request.id(extractWikimediaId(json));
        request.source(json, XContentType.JSON);
        return request;
    }

    private String extractWikimediaId(String json) {
        try {
            return objectMapper.readTree(json).get("meta").get("id").asText();
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    private RestClientBuilder createRestClient() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(getUsername(), getPassword()));
        return RestClient.builder(HttpHost.create(getServerUrl()))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
    }
}
