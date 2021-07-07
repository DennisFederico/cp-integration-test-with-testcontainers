package io.confluent.testcontainers;

import org.junit.ClassRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Test basic capacities and usage for testing of testcontainers")
@Testcontainers
public class SimplestContainerTest {

    private static final DockerImageName TINY_IMAGE = DockerImageName.parse("alpine").withTag("3.5");
    private static Logger logger = LoggerFactory.getLogger(SimplestContainerTest.class);

    @ClassRule
    public static Network testNetwork = Network.newNetwork();

    @ClassRule
    public static GenericContainer<?> httpServerContainer =
            new GenericContainer<>(TINY_IMAGE)
                    .withExposedPorts(8080)
                    .withNetwork(testNetwork)
                    .withNetworkAliases("webserver")
                    .withLogConsumer(new Slf4jLogConsumer(logger))
                    .withCommand("/bin/sh", "-c", "while true ; do printf 'HTTP/1.1 200 OK\\n\\nHello There!' | nc -l -p 8080; done");

    @Test
    public void callFromInsideDockerNetwork() {
        try (GenericContainer<?> httpClientContainer = new GenericContainer<>(TINY_IMAGE)
                .withNetwork(testNetwork)
                .withCommand("top")) {
            httpServerContainer.start();
            httpClientContainer.start();
            String address = "webserver";
            Integer port = 8080;
            String response = httpClientContainer.execInContainer("wget", "-O", "-", String.format("http://%s:%d", address, port)).getStdout();
            assertEquals("Hello There!", response, "received response");
        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void outsideCallFromHost() {
        try {
            httpServerContainer.start();

            String address = httpServerContainer.getHost();
            Integer port = httpServerContainer.getMappedPort(8080);

            HttpClient httpClient = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder(new URI(String.format("http://%s:%d", address, port))).build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals("Hello There!", response.body(), "received response");
        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void insideLoopbackCallTest() {
        //We need to start the container before exposing it as an outside service (loopback)
        httpServerContainer.start();
        assertTrue(httpServerContainer.isRunning());
        //Expose the container as it its in the host, note the mappedPort
        org.testcontainers.Testcontainers.exposeHostPorts(httpServerContainer.getMappedPort(8080));

        //The calling container from inside docker placed in a different docker network
        try (GenericContainer<?> httpClientContainer = new GenericContainer<>(TINY_IMAGE)
                .withNetwork(Network.newNetwork())
                .withCommand("top")) {

            httpClientContainer.start();
            assertTrue(httpClientContainer.isRunning());
            String requestUrl = "http://" + GenericContainer.INTERNAL_HOST_HOSTNAME + ":" + httpServerContainer.getMappedPort(8080);
            String response = httpClientContainer.execInContainer("wget", "-O", "-", requestUrl).getStdout();
            assertEquals("Hello There!", response, "received response");
        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void networkedContainersTest() {
        try (Network network = Network.newNetwork();
             GenericContainer<?> serverContainer = new GenericContainer<>(TINY_IMAGE)
                     .withNetwork(network)
                     .withNetworkAliases("foo")
                     .withCommand("/bin/sh", "-c", "while true ; do printf 'HTTP/1.1 200 OK\\n\\nHello World!' | nc -l -p 8080; done");
             GenericContainer<?> clientContainer = new GenericContainer<>(TINY_IMAGE)
                     .withNetwork(network)
                     .withCommand("top")) {

            serverContainer.start();
            assertTrue(serverContainer.isRunning());
            clientContainer.start();
            assertTrue(clientContainer.isRunning());

            String response = clientContainer.execInContainer("wget", "-O", "-", "http://foo:8080").getStdout();
            assertEquals("Hello World!", response, "received response");
        } catch (Exception e) {
            fail(e);
        }
    }

}
