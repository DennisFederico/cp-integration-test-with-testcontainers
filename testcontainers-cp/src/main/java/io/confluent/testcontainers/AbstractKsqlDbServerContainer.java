package io.confluent.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;

public class AbstractKsqlDbServerContainer<T extends GenericContainer<T>> extends GenericContainer<T> {

    public static final int DEFAULT_PORT = 8088;
    public static final String DEFAULT_NETWORK_ALIAS = "ksql-server";

    public AbstractKsqlDbServerContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public AbstractKsqlDbServerContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        withNetworkAliases(DEFAULT_NETWORK_ALIAS);
        withEnv("KSQL_HOST_NAME", DEFAULT_NETWORK_ALIAS);
        withEnv("KSQL_LISTENERS", "http://0.0.0.0:" + DEFAULT_PORT);
        withExposedPorts(DEFAULT_PORT);
        waitingFor(forLogMessage(".*INFO Server up and running.*\\n", 1));
    }

    /**
     * Will create a "depends" on the container and bind the "internal" bootstrapserver listener and binds
     * to the same network of the KafkaContainer.
     * To set an external kafka use withKafka method
     * @param kafkaContainer
     * @return
     */
    public T withKafkaContainer(KafkaContainer kafkaContainer) {
        return dependsOn(kafkaContainer)
                .withNetwork(kafkaContainer.getNetwork())
                //KafkaContainer "internal" port is hardcoded to 9092
                .withEnv("KSQL_BOOTSTRAP_SERVERS", "PLAINTEXT://" + kafkaContainer.getNetworkAliases().get(0) + ":9092");
    }

    /**
     * Use to configure an external kafka
     * @param bootstrapServers
     * @return
     */
    public T withKafka(String bootstrapServers) {
        return withEnv("KSQL_BOOTSTRAP_SERVERS", bootstrapServers);
    }

    /**
     *
     * @param schemaRegistryContainer
     * @return
     */
    public T withSchemaRegistryContainer(SchemaRegistryContainer schemaRegistryContainer) {
        return dependsOn(schemaRegistryContainer)
                .withEnv("KSQL_KSQL_SCHEMA_REGISTRY_URL", schemaRegistryContainer.getSchemaRegistryInternalNetworkUrl());
    }

    /**
     * Use to configure an "external" schemaRegistry
     *
     * @param schemaRegistryUrl
     * @return
     */
    public T withSchemaRegistry(String schemaRegistryUrl) {
        return withEnv("KSQL_KSQL_SCHEMA_REGISTRY_URL", schemaRegistryUrl);
    }

    public String getKsqlDbUrl() {
        return "http://" + getContainerIpAddress() + ":" + getMappedPort(DEFAULT_PORT);
    }
}
