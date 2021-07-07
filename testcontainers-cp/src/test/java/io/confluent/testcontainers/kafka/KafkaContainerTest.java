package io.confluent.testcontainers.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Testing capabilities and usage of testcontainers KafkaContainer")
@Testcontainers
public class KafkaContainerTest {

    Logger logger = LoggerFactory.getLogger(KafkaContainerTest.class);

    public static Network kafkaNetwork = Network.newNetwork();

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka").withTag("6.2.0-1-ubi8"))
            .withReuse(false)
            .withNetwork(kafkaNetwork)
            .withNetworkAliases("kafka");

    @Test
    public void kafkaUpTest(TestInfo testinfo) {
        logger.info("Test {} - started", testinfo.getDisplayName());
        kafka.start();
        assertTrue(kafka.isRunning());
        logger.info("Test {} - finished", testinfo.getDisplayName());
    }

    @Test
    public void kafkaUpWithExposedHostPortAfterContainerBuildTest() {
        org.testcontainers.Testcontainers.exposeHostPorts(12345);
        kafka.start();
        assertTrue(kafka.isRunning());
    }

    @Test
    public void kafkaUpWithExposedHostPortAndDefaultNetworkBeforeContainerBuildTest() {
        org.testcontainers.Testcontainers.exposeHostPorts(12345);
        KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka").withTag("6.2.0-1-ubi8"))
                .withReuse(false)
                .withNetworkAliases("kafka");
        kafka.start();
        assertTrue(kafka.isRunning());
    }

    @Disabled("as of version 1.15.3 of org.testcontainers:kafka exposing a host port before creating the kafka container with specific network fails")
    @Test
    public void kafkaUpWithExposedHostPortAndWithSpecificNetworkBeforeContainerBuildTest() {
        org.testcontainers.Testcontainers.exposeHostPorts(12345);
        KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka").withTag("6.2.0-1-ubi8"))
                .withReuse(false)
                .withNetwork(Network.newNetwork())
                .withNetworkAliases("kafka");
        kafka.start();
        assertTrue(kafka.isRunning());
    }

    @Test
    public void kafkaWorkingFromHostNetwork() {
        try (AdminClient adminClient = AdminClient.create(
                Map.of(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                        kafka.getBootstrapServers()
                ));

             KafkaProducer<String, String> producer = new KafkaProducer<>(
                     Map.of(
                             ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                             ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                     ), new StringSerializer(), new StringSerializer()

             );

             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                     Map.of(
                             ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                             ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                             ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                     ), new StringDeserializer(), new StringDeserializer()
             )
        ) {
            String topicName = "topic-" + UUID.randomUUID();
            Collection<NewTopic> topics = singletonList(new NewTopic(topicName, 1, (short) 1));
            adminClient.createTopics(topics).all().get(10, TimeUnit.SECONDS);

            consumer.subscribe(singletonList(topicName));
            producer.send(new ProducerRecord<>(topicName, "test", "containers")).get();

            ConsumerRecords<String, String> records;
            int retries = 50;
            do {
                records = consumer.poll(Duration.ofMillis(100));
            } while (--retries > 0 && (records == null || records.isEmpty()));

            assertThat(records, notNullValue());
            assertFalse(records.isEmpty());
            Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
            assertTrue(iterator.hasNext());
            ConsumerRecord<String, String> record = iterator.next();
            assertEquals("test", record.key());
            assertEquals("containers", record.value());
            assertEquals(topicName, record.topic());
            assertFalse(iterator.hasNext());
            consumer.unsubscribe();
        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void kafkaFunctioningFromDockerNetwork() {
        try (GenericContainer<?> kafkaClientContainer = new GenericContainer<>(DockerImageName.parse("cnfltraining/training-tools").withTag("6.0"))
                .withNetwork(kafkaNetwork)
                .withCreateContainerCmdModifier(cmd -> cmd.withAttachStdin(true)
                        .withStdinOpen(true)
                        .withTty(true))) {
            kafka.start();
            assertTrue(kafka.isRunning());

            kafkaClientContainer.start();
            String bootstrap = kafka.getNetworkAliases().get(0) + ":9092";
            org.testcontainers.containers.Container.ExecResult execResult = kafkaClientContainer.execInContainer("kafka-topics", "--create", "--bootstrap-server", bootstrap, "--topic", "myTopic");
            assertEquals(0, execResult.getExitCode());
            assertThat(execResult.getStdout(), allOf(containsString("myTopic"), containsString("Created")));

            execResult = kafkaClientContainer.execInContainer("kafka-topics", "--list", "--bootstrap-server", bootstrap);
            assertThat(execResult.getStdout(), containsString("myTopic"));
        } catch (Exception e) {
            fail(e);
        }
    }
}

