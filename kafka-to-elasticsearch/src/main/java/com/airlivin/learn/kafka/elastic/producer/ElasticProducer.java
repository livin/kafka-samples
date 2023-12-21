package com.airlivin.learn.kafka.elastic.producer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

public class ElasticProducer {
    private static final Logger log = LoggerFactory.getLogger(ElasticProducer.class);

    private final Properties props;

    public ElasticProducer(Properties props) {
        this.props = props;
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

    public void stream(Stream<String> stream) {
        RestHighLevelClient client = new RestHighLevelClient(createRestClient());
            stream.forEach(s -> {
                try {
                    IndexRequest request = new IndexRequest("wikimedia");
                    request.id(UUID.randomUUID().toString());
                    request.source(s, XContentType.JSON);
                    IndexResponse response = client.index(request, RequestOptions.DEFAULT);
                    log.info(STR."Indexed \{response.getId()} with version \{response.getVersion()}");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
    }

    private RestClientBuilder createRestClient() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(getUsername(), getPassword()));
        return RestClient.builder(HttpHost.create(getServerUrl()))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
    }
}
