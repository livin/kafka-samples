## Kafka to Elastic Search streamer

### Notes on Java

In this project we experiment with Java 21 String Templates.
So it requires to use Java 21 and enable preview options on compilation and running.

### Elastic search service and client version
I use Elastic search 7.10 service from bonsai and client.

Elastic has new version of Java API client - 8.* - but it's can't be used with
free bonsai's Elastic 7.10.

Yet API is almost the same as for OpenSearch (AWS fork of Elastic).