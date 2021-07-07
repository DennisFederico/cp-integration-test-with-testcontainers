package io.confluent.testcontainers.cluster;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;

import static io.restassured.RestAssured.get;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@DisplayName("Kafka Cluster Up and Running with TestContainers bootstrapped with docker-compose")
@Testcontainers
public class KafkaClusterComposeTest {

    private static final File composeFile = new File(KafkaClusterComposeTest.class.getClassLoader()
            .getResource("compose-kafka-cluster.yml").getFile());

    @Container
    public static DockerComposeContainer kafkaComposeCluster = new DockerComposeContainer(composeFile)
            .withExposedService("broker", 29092)
            .withExposedService("schema-registry", 8081, Wait.forHttp("/subjects").forStatusCode(200))
            .withExposedService("ksqldb-server", 8088, Wait.forLogMessage(".*INFO Server up and running.*\\n", 1));

    @Test
    public void myComposeTest() {
        try {
            kafkaComposeCluster.start();

            Optional<ContainerState> kafka = kafkaComposeCluster.getContainerByServiceName("broker_1");
            assertThat(kafka.isPresent(), is(true));
            assertThat(kafka.get().isRunning(), is(true));
            Optional<ContainerState> schemaRegistry = kafkaComposeCluster.getContainerByServiceName("schema-registry_1");
            assertThat(schemaRegistry.isPresent(), is(true));
            assertThat(schemaRegistry.get().isRunning(), is(true));
            Optional<ContainerState> ksqlDbServer = kafkaComposeCluster.getContainerByServiceName("ksqldb-server_1");
            assertThat(ksqlDbServer.isPresent(), is(true));
            assertThat(ksqlDbServer.get().isRunning(), is(true));

            //TEST SCHEMA REGISTRY UP - Two ways of fetching host/port
            String schemaRegistryHost = schemaRegistry.get().getHost();
            int schemaRegistryPort = kafkaComposeCluster.getServicePort("schema-registry", 8081);
            String srUrl = "http://" + schemaRegistryHost + ":" + schemaRegistryPort + "/subjects";

            HttpClient httpClient = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(srUrl))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            assertEquals("[]", response.body());

            //KSQL-DB UP - Two ways of fetching host/port
            String ksqlDbHost = kafkaComposeCluster.getServiceHost("ksqldb-server", 8088);
            int ksqlDbPort = ksqlDbServer.get().getMappedPort(8088);
            String ksqlDbInfoUrl = "http://" + ksqlDbHost + ":" + ksqlDbPort + "/info";
            get(ksqlDbInfoUrl)
                    .then()
                    .body("KsqlServerInfo.serverStatus", equalTo("RUNNING"));

        } catch (Exception e) {
            fail(e);
        }
    }
}

