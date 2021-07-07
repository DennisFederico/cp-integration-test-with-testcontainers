package io.confluent.testcontainers;

import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import java.util.Optional;

public class KafkaContainersCluster implements TestRule, Startable {

    private final RuleChain delegate;
    private Network network;
    private KafkaContainer kafkaContainer;
    private SchemaRegistryContainer schemaRegistryContainer;
    private AbstractKsqlDbServerContainer ksqlDbServerContainer;

    private KafkaContainersCluster(Network network, KafkaContainer kafkaContainer, SchemaRegistryContainer schemaRegistryContainer, AbstractKsqlDbServerContainer ksqlDbServerContainer) {
        this.network = network;
        this.kafkaContainer = kafkaContainer;
        this.schemaRegistryContainer = schemaRegistryContainer;
        this.ksqlDbServerContainer = ksqlDbServerContainer;
        delegate = RuleChain.outerRule(kafkaContainer)
                .around(schemaRegistryContainer)
                .around(ksqlDbServerContainer);
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return delegate.apply(base, description);
    }

    public Network getNetwork() {
        return network;
    }

    public KafkaContainer getKafkaContainer() {
        return kafkaContainer;
    }

    public SchemaRegistryContainer getSchemaRegistryContainer() {
        return schemaRegistryContainer;
    }

    public AbstractKsqlDbServerContainer getKsqlDbServerContainer() {
        return ksqlDbServerContainer;
    }

    @Override
    public void start() {
        ksqlDbServerContainer.start();
    }

    @Override
    public void stop() {
        ksqlDbServerContainer.start();
        schemaRegistryContainer.stop();
        kafkaContainer.stop();
    }

    public static class KafkaClusterBuilder {
        private Optional<Network> optionalNetwork = Optional.empty();

        public KafkaClusterBuilder withNetwork(Network network) {
            this.optionalNetwork = Optional.of(network);
            return this;
        }

        public KafkaContainersCluster build() {
            //All the containers are set to the same network as the adopt the network set in the Kafka container
            Network network = optionalNetwork.orElseGet(Network::newNetwork);

            KafkaContainer kafkaContainer =
                    new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.0-1-ubi8"))
                            .withNetwork(network)
                            .withNetworkAliases("kafka")
                            .withReuse(false);

            SchemaRegistryContainer schemaRegistryContainer =
                    new SchemaRegistryContainer(SchemaRegistryContainer.DEFAULT_IMAGE_NAME.withTag(SchemaRegistryContainer.DEFAULT_IMAGE_TAG))
                            .withKafka(kafkaContainer)
                            .withReuse(false);

            AbstractKsqlDbServerContainer ksqlDbServerContainer =
                    new CpKsqlDbServerContainer(CpKsqlDbServerContainer.DEFAULT_IMAGE_NAME.withTag(CpKsqlDbServerContainer.DEFAULT_IMAGE_TAG))
                            .withKafkaContainer(kafkaContainer)
                            .withSchemaRegistryContainer(schemaRegistryContainer)
                            .withReuse(false);

            return new KafkaContainersCluster(network, kafkaContainer, schemaRegistryContainer, ksqlDbServerContainer);
        }
    }
}
