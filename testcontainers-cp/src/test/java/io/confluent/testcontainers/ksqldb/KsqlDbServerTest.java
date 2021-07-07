package io.confluent.testcontainers.ksqldb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.confluent.ksql.api.client.*;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.testcontainers.CpKsqlDbServerContainer;
import io.confluent.testcontainers.SchemaRegistryContainer;
import io.confluent.testcontainers.movies.Movie;
import io.confluent.testcontainers.movies.MoviesSupport;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Testing capabilities and usage of adhoc SchemaRegistryContainer based on Testcontainers")
@Testcontainers
public class KsqlDbServerTest {

    private static final Logger logger = LoggerFactory.getLogger(KsqlDbServerTest.class);
    private static final String KSQLDB_REQUEST_CONTENT_TYPE = "application/vnd.ksql.v1+json";
    private static final Network network = Network.newNetwork();

    @Container
    public final KafkaContainer kafkaContainer = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka")
                    .withTag("6.2.0-1-ubi8"))
            .withReuse(false)
            .withNetwork(network);

    @Container
    public final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer(
            SchemaRegistryContainer.DEFAULT_IMAGE_NAME.withTag(SchemaRegistryContainer.DEFAULT_IMAGE_TAG))
            .withReuse(false)
            .withKafka(kafkaContainer);

    @Container
    public final CpKsqlDbServerContainer ksqlDbServerContainer = new CpKsqlDbServerContainer(
            CpKsqlDbServerContainer.DEFAULT_IMAGE_NAME.withTag(CpKsqlDbServerContainer.DEFAULT_IMAGE_TAG))
            .withReuse(false)
            .withLogConsumer(new Slf4jLogConsumer(logger))
            .withKafkaContainer(kafkaContainer)
            .withSchemaRegistryContainer(schemaRegistryContainer);

    @Test
    public void ksqlDbServerUpTest() {
        try {
            ksqlDbServerContainer.start();
            assertTrue(ksqlDbServerContainer.isRunning());

            HttpClient httpClient = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(ksqlDbServerContainer.getKsqlDbUrl() + "/info"))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("RUNNING"));

            //THIS IS EASIER
            get(ksqlDbServerContainer.getKsqlDbUrl() + "/info")
                    .then()
                    .body("KsqlServerInfo.serverStatus", equalTo("RUNNING"));

            //RUNNING WITH SCHEMA REGISTRY CONFIGURED
            String ksqlDbRequest = createKsqlDbJsonRequest("show properties;", null);

            JsonPath jsonPath = given()
                    .body(ksqlDbRequest)
                    .contentType(KSQLDB_REQUEST_CONTENT_TYPE)
                    .when()
                    .post(ksqlDbServerContainer.getKsqlDbUrl() + "/ksql")
                    .then()
                    .contentType(ContentType.JSON)
                    .extract()
                    .response()
                    .jsonPath();

            Map<String, String> srPropertyValue = jsonPath.get("[0].properties.find{it['name'] == 'ksql.schema.registry.url'}");
            assertThat(srPropertyValue, not(anEmptyMap()));
            assertThat(srPropertyValue, hasEntry(is("value"), is("http://" + SchemaRegistryContainer.DEFAULT_NETWORK_ALIAS + ":" + SchemaRegistryContainer.DEFAULT_PORT)));
        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void shouldCreateStreamFromATopicAndConsumeDataWithSchema() {

        try {
            ksqlDbServerContainer.start();
            assertTrue(ksqlDbServerContainer.isRunning());

            //Produce some data that will have a schema registered
            Producer<String, Movie> jsonProducer = MoviesSupport.createJsonProducer(kafkaContainer.getBootstrapServers(), schemaRegistryContainer.getSchemaRegistryUrl());
            MoviesSupport.publishData(MoviesSupport.MOVIES, jsonProducer, "movies_topic");

            //Confirm the schema existence
            HttpClient httpClient = HttpClient.newHttpClient();
            HttpRequest subjectsRequest = HttpRequest.newBuilder()
                    .uri(URI.create(schemaRegistryContainer.getSchemaRegistryUrl() + "/subjects"))
                    .build();
            HttpResponse<String> subjects = httpClient.send(subjectsRequest, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, subjects.statusCode());
            assertThat(subjects.body(), containsString("movies_topic-value"));

            //Create the stream "mind the offset reset"
            String createStreamStmt = "CREATE STREAM movies_json_stream (" +
                    "movie_id INT," +
                    "title VARCHAR," +
                    "release_year INT," +
                    "producer_id INT" +
                    ") WITH (" +
                    "KAFKA_TOPIC='movies_topic', VALUE_FORMAT='JSON_SR'" +
                    ");";

            String ksqlDbJsonRequest = createKsqlDbJsonRequest(createStreamStmt, Collections.singletonMap("ksql.streams.auto.offset.reset", "earliest"));
            Map<String, String> status = given()
                    .body(ksqlDbJsonRequest)
                    .contentType(KSQLDB_REQUEST_CONTENT_TYPE)
                    .when()
                    .post(ksqlDbServerContainer.getKsqlDbUrl() + "/ksql")
                    .then()
                    .contentType(ContentType.JSON)
                    .extract()
                    .path("[0].commandStatus");
            assertThat(status, allOf(hasEntry("status", "SUCCESS"), hasEntry("message", "Stream created")));

            //CONFIRM STREAM WAS CREATED
            Map<String, String> streams = given()
                    .body(createKsqlDbJsonRequest("LIST STREAMS;", null))
                    .contentType(KSQLDB_REQUEST_CONTENT_TYPE)
                    .when()
                    .post(ksqlDbServerContainer.getKsqlDbUrl() + "/ksql")
                    .then()
                    .contentType(ContentType.JSON)
                    .extract()
                    .path("[0].streams[0]");
            assertThat(streams, allOf(hasEntry("type", "STREAM"), hasEntry("name", "MOVIES_JSON_STREAM"), hasEntry("valueFormat", "JSON_SR"), hasEntry("topic", "movies_topic")));

            //Same confirmation as above but using ksqldb client
            ClientOptions clientOptions = ClientOptions.create()
                    .setHost(ksqlDbServerContainer.getContainerIpAddress())
                    .setPort(ksqlDbServerContainer.getMappedPort(CpKsqlDbServerContainer.DEFAULT_PORT));
            Client client = Client.create(clientOptions);
            List<StreamInfo> streamInfos = client.listStreams().get();
            assertThat(streamInfos, not(empty()));
            assertThat(streamInfos.get(0), allOf(
                    hasProperty("name", is("MOVIES_JSON_STREAM")),
                    hasProperty("valueFormat", is("JSON_SR")),
                    hasProperty("topic", is("movies_topic"))
            ));

            //QUERY THE STREAM
            List<Row> rows = client.executeQuery("SELECT * FROM MOVIES_JSON_STREAM EMIT CHANGES LIMIT " +
                            MoviesSupport.MOVIES.size() + ";",
                    Collections.singletonMap("ksql.streams.auto.offset.reset", "earliest")
            ).get(30, TimeUnit.SECONDS);
            //KSQL COLUMNS ARE UPPER CASE
            ObjectMapper mapper = new ObjectMapper().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES);
            List<Movie> movies = rows.stream()
                    .map(Row::asObject)
                    .map(KsqlObject::toJsonString)
                    .map(json -> {
                        try {
                            return mapper.readValue(json, Movie.class);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList());
            assertThat(movies, contains(MoviesSupport.MOVIES.toArray()));

        } catch (Exception e) {
            fail(e);
        }
    }

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new Jdk8Module());

    private String createKsqlDbJsonRequest(final String statement, final Map<String, ?> params) throws JsonProcessingException {
        return objectMapper.writeValueAsString(new KsqlRequest(statement, null, params, null));
    }
}
