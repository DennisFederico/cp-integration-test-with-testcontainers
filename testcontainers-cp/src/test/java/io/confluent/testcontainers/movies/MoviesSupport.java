package io.confluent.testcontainers.movies;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.testcontainers.movies.support.LoggingConsumerInterceptor;
import io.confluent.testcontainers.movies.support.LoggingProducerInterceptor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MoviesSupport {
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new Jdk8Module());

    public static final List<Movie> MOVIES = readData("movies/movies.data", Movie.class);
    public static final List<MovieProducer> PRODUCERS = readData("movies/producers.data", MovieProducer.class);

    private static<T>List<T> readData(String file, Class<T> clazz) {
        try {
            File dataFile = new File(MoviesSupport.class.getClassLoader().getResource(file).getFile());
            return Files.lines(dataFile.toPath())
                    .map(line -> {
                        try {
                            return objectMapper.readValue(line, clazz);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private static Properties baseJsonProducerProperties(String bootstrapServer, String schemaRegistryURL) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);

        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Collections.singletonList(LoggingProducerInterceptor.class));
        return props;
    }

    @NotNull
    private static Properties baseJsonConsumerProperties(String bootstrapServer, String schemaRegistryUrl) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        String consumerId = Long.toString(UUID.randomUUID().getLeastSignificantBits(), Character.MAX_RADIX);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "movieConsumer-" + consumerId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "250");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class);
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, Collections.singletonList(LoggingConsumerInterceptor.class));
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, Movie.class.getName());
        return props;
    }

    public static <T> Producer<String, T> createJsonProducer(String bootstrapServer, String schemaRegistryURL) {
        return new KafkaProducer<>(baseJsonProducerProperties(bootstrapServer, schemaRegistryURL));
    }

    public static <T> Consumer<String, Movie> createJsonConsumer(String bootstrapServer, String schemaRegistryUrl) {
        return new KafkaConsumer<>(baseJsonConsumerProperties(bootstrapServer, schemaRegistryUrl));
    }

    public static void createTopic(String bootstrapServers, String topicName) {
        try (AdminClient adminClient =
                     AdminClient.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
            adminClient.createTopics(Collections.singleton(newTopic));
        }
    }

    private static Function<Future<RecordMetadata>, CompletableFuture<RecordMetadata>> toCompletableFuture = future -> CompletableFuture.supplyAsync((Supplier<RecordMetadata>) () -> {
        try {
            return future.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    });

    public static <T> List<RecordMetadata> publishData(List<T> data, Producer<String, T> producer, String topic) {
        return data.stream()
                .map(d -> producer.send(new ProducerRecord<>(topic, d)))
                .map(toCompletableFuture)
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
    }
}
