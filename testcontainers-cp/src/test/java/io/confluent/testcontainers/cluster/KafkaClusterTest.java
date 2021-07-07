package io.confluent.testcontainers.cluster;

import io.confluent.testcontainers.KafkaContainersCluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static io.restassured.RestAssured.get;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Kafka Cluster Up and Running using KafkaContainersCluster class")
@Testcontainers
public class KafkaClusterTest {

    private static Logger logger = LoggerFactory.getLogger(KafkaClusterTest.class);

    @Container
    public static final KafkaContainersCluster kafkaCluster = new KafkaContainersCluster.KafkaClusterBuilder().build();

    @Test
    public void clusterUpAndServicingTest() {
        try {
            kafkaCluster.start();
            assertTrue(kafkaCluster.getKafkaContainer().isRunning());
            assertTrue(kafkaCluster.getSchemaRegistryContainer().isRunning());
            assertTrue(kafkaCluster.getKsqlDbServerContainer().isRunning());

            //TEST SCHEMA REGISTRY UP
            String srUrl = kafkaCluster.getSchemaRegistryContainer().getSchemaRegistryUrl() + "/subjects";

            HttpClient httpClient = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(srUrl))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            assertEquals("[]", response.body());

            //KSQL-DB UP
            String ksqlDbUrl = kafkaCluster.getKsqlDbServerContainer().getKsqlDbUrl() + "/info";
            get(ksqlDbUrl)
                    .then()
                    .body("KsqlServerInfo.serverStatus", equalTo("RUNNING"));
        } catch (Exception e) {
            fail(e);
        }
    }
}
