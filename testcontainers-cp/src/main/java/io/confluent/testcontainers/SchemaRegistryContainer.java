package io.confluent.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * This container wraps Confluent Schema Registry
 * To learn more about Schema Registry https://docs.confluent.io/current/schema-registry/schema_registry_tutorial.html
 * <p>
 * Inspired by https://github.com/gAmUssA/testcontainers-java-module-confluent-platform
 */
public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

    public static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("confluentinc/cp-schema-registry");
    public static final String DEFAULT_IMAGE_TAG = "6.2.0-1-ubi8";
    public static final int DEFAULT_PORT = 8081;
    public static final String DEFAULT_NETWORK_ALIAS = "schema-registry";

    public SchemaRegistryContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public SchemaRegistryContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
        withNetworkAliases(DEFAULT_NETWORK_ALIAS);
        withExposedPorts(DEFAULT_PORT);
        withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry");
        withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + DEFAULT_PORT);
        waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
    }

    /**
     * This method will configure the schema-registry to use the provided kafka container
     * it will use the same network and add it as dependency during start.
     *
     * @param kafkaContainer
     * @return
     */
    public SchemaRegistryContainer withKafka(KafkaContainer kafkaContainer) {
        //KafkaContainer "internal" port is hardcoded to 9092
        return dependsOn(kafkaContainer)
                .withNetwork(kafkaContainer.getNetwork())
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + kafkaContainer.getNetworkAliases().get(0) + ":9092");
    }

    /**
     * To connect to a kafka in a different network. Provide the bootstrapserver including the listener
     * ie. PLAINTEXT://kafka-broker-host:kafka-broker-port-map
     *
     * @param bootstrapServers
     * @return
     */
    public SchemaRegistryContainer withKafka(String bootstrapServers) {
        return withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", bootstrapServers);
    }

    /**
     * returns the URL as exposed to the docker host, the container must be running to properly resolver the ports
     * use the DEFAULT_NETWORK_ALIAS and DEFAULT_PORT if you want the "internal" to the container docker network address
     * @return Schema Registry URL
     */
    public String getSchemaRegistryUrl() {
        return String.format("http://%s:%s", getContainerIpAddress(), getMappedPort(DEFAULT_PORT));
    }

    public String getSchemaRegistryInternalNetworkUrl() {
        return String.format("http://%s:%s", DEFAULT_NETWORK_ALIAS, DEFAULT_PORT);
    }
}