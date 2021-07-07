package io.confluent.testcontainers.schemaregistry;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.testcontainers.SchemaRegistryContainer;
import io.confluent.testcontainers.movies.Movie;
import io.confluent.testcontainers.movies.MoviesSupport;
import io.restassured.http.ContentType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Testing capabilities and usage of adhoc SchemaRegistryContainer based on Testcontainers")
@Testcontainers
public class SchemaRegistryTest {

    private static Logger logger = LoggerFactory.getLogger(SchemaRegistryTest.class);

    public static final Network network = Network.newNetwork();

    @Container
    public static final KafkaContainer kafkaContainer = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka")
                    .withTag("6.2.0-1-ubi8"))
            .withReuse(false)
            .withNetwork(network);

    @Container
    public static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer(
            SchemaRegistryContainer.DEFAULT_IMAGE_NAME
                    .withTag(SchemaRegistryContainer.DEFAULT_IMAGE_TAG))
            .withReuse(false)
            .withLogConsumer(new Slf4jLogConsumer(logger))
            .withKafka(kafkaContainer);

    @Test
    public void schemaRegistryUpTest() {
        try {
            schemaRegistryContainer.start();
            assertTrue(schemaRegistryContainer.isRunning());

            HttpClient httpClient = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(schemaRegistryContainer.getSchemaRegistryUrl() + "/subjects"))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void schemaRegistryWorkFromHostNetwork() {
        try {
            schemaRegistryContainer.start();
            assertTrue(schemaRegistryContainer.isRunning());

            CachedSchemaRegistryClient srClient = new CachedSchemaRegistryClient(
                    Collections.singletonList(schemaRegistryContainer.getSchemaRegistryUrl()),
                    3, List.of(new JsonSchemaProvider(), new AvroSchemaProvider()),
                    null);

            String avroSchema = "{\"type\":\"record\",\"name\":\"myRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";
            Optional<ParsedSchema> parsedAvroSchema = srClient.parseSchema("AVRO", avroSchema, Collections.emptyList());
            srClient.register("myAvroSchema", parsedAvroSchema.get());

            String jsonSchema = "{\"type\":\"object\",\"properties\":{\"field1\":{\"type\":\"string\"}}}";
            Optional<ParsedSchema> parsedJsonSchema = srClient.parseSchema("JSON", jsonSchema, Collections.emptyList());
            srClient.register("myJsonSchema", parsedJsonSchema.get());

            HttpClient httpClient = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(schemaRegistryContainer.getSchemaRegistryUrl() + "/subjects"))
                    .build();
            HttpResponse<String> subjects = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, subjects.statusCode());
            assertThat(subjects.body(), allOf(containsString("myAvroSchema"), containsString("myJsonSchema")));

            ParsedSchema myAvroSchema = srClient.getSchemaById(srClient.getId("myAvroSchema", parsedAvroSchema.get()));
            assertTrue(parsedAvroSchema.get().deepEquals(myAvroSchema));

            ParsedSchema myJsonSchema = srClient.getSchemaById(srClient.getId("myJsonSchema", parsedJsonSchema.get()));
            assertTrue(parsedJsonSchema.get().deepEquals(myJsonSchema));

            given()
                    .get(schemaRegistryContainer.getSchemaRegistryUrl() + "/subjects/myJsonSchema/versions/1")
                    .then()
                    .contentType(ContentType.JSON)
                    .body("subject", is("myJsonSchema"))
                    .body("version", is(1))
                    .body("schemaType", is("JSON"));

            given()
                    .get(schemaRegistryContainer.getSchemaRegistryUrl() + "/subjects/myJsonSchema/versions/1/schema")
                    .then()
                    .contentType(ContentType.JSON)
                    .body(is(jsonSchema));

        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void produceAndConsumeEventsWithSchema() {
        try {
            schemaRegistryContainer.start();
            assertTrue(schemaRegistryContainer.isRunning());

            MoviesSupport.createTopic(kafkaContainer.getBootstrapServers(), "movieTopic");
            Producer<String, Movie> jsonProducer = MoviesSupport.createJsonProducer(kafkaContainer.getBootstrapServers(), schemaRegistryContainer.getSchemaRegistryUrl());
            MoviesSupport.publishData(MoviesSupport.MOVIES, jsonProducer, "movieTopic");

            //Check that a schema was registered for values in the topic
            HttpClient httpClient = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(schemaRegistryContainer.getSchemaRegistryUrl() + "/subjects"))
                    .build();
            HttpResponse<String> subjects = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, subjects.statusCode());
            assertThat(subjects.body(), containsString("movieTopic-value"));

            Consumer<String, Movie> jsonConsumer = MoviesSupport.createJsonConsumer(kafkaContainer.getBootstrapServers(), schemaRegistryContainer.getSchemaRegistryUrl());
            jsonConsumer.subscribe(Collections.singletonList("movieTopic"));
            ConsumerRecords<String, Movie> poll;
            List<Movie> movies = new ArrayList<>();
            do {
                poll = jsonConsumer.poll(Duration.ofSeconds(1));
                poll.forEach(r -> movies.add(r.value()));
            } while (!poll.isEmpty());

            assertThat(movies, contains(MoviesSupport.MOVIES.toArray()));

        } catch (Exception e) {
            fail(e);
        }
    }
}
