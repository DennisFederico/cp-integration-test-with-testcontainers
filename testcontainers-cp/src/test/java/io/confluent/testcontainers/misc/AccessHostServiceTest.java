package io.confluent.testcontainers.misc;

import com.github.dockerjava.api.model.NetworkSettings;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;

import java.io.OutputStream;
import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
@Disabled ("This suit exposes a Host port which creates a network that breaks kafkaContainer config - Pending PR approve to fix this")
public class AccessHostServiceTest {

    private static final DockerImageName TINY_IMAGE = DockerImageName.parse("alpine:3.5");
    private static Logger logger = LoggerFactory.getLogger(AccessHostServiceTest.class);

    private static HttpServer HTTP_SERVER;
    private static int INTERNAL_PORT = 8181;

    @BeforeAll
    public static void setUpHostServer() throws Exception {
        logger.info("FIRING EXTERNAL SERVER");
        HTTP_SERVER = HttpServer.create(new InetSocketAddress(0), 0);
        HTTP_SERVER.createContext("/", exchange -> {
            byte[] content = "Hello There!".getBytes();
            exchange.sendResponseHeaders(200, content.length);
            try (OutputStream responseBody = exchange.getResponseBody()) {
                responseBody.write(content);
                responseBody.flush();
            }
        });

        HTTP_SERVER.start();
        org.testcontainers.Testcontainers.exposeHostPorts(ImmutableMap.of(HTTP_SERVER.getAddress().getPort(), INTERNAL_PORT));
    }

    @AfterAll
    public static void clearExposedPorts() {
        //This cannot be done yet, perhaps it would be a good feature to implement
        //PortForwardingContainer.INSTANCE.exposeHostPort(entry.getKey(), entry.getValue());
    }

    @Test
    public void accessHostServiceFromDefaultNetworkTest() {
        try (GenericContainer<?> httpClientContainer = new GenericContainer<>(TINY_IMAGE)
                .withCommand("top")) {
            httpClientContainer.start();
            String requestUrl = GenericContainer.INTERNAL_HOST_HOSTNAME + ":" + INTERNAL_PORT;
            String response = httpClientContainer.execInContainer("wget", "-O", "-", requestUrl).getStdout();
            assertEquals("Hello There!", response, "received response");
        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void accessHostServiceFromSpecificNetworkTest() {
        try (GenericContainer<?> httpClientContainer = new GenericContainer<>(TINY_IMAGE)
                .withNetwork(Network.newNetwork())
                .withCommand("top")) {
            httpClientContainer.start();
            NetworkSettings networkSettings = httpClientContainer.getContainerInfo().getNetworkSettings();

            String requestUrl = GenericContainer.INTERNAL_HOST_HOSTNAME + ":" + INTERNAL_PORT;
            String response = httpClientContainer.execInContainer("wget", "-O", "-", requestUrl).getStdout();
            assertEquals("Hello There!", response, "received response");
        } catch (Exception e) {
            fail(e);
        }
    }
}
