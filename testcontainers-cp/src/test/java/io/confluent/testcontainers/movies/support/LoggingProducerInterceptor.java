package io.confluent.testcontainers.movies.support;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class LoggingProducerInterceptor implements ProducerInterceptor {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LoggingProducerInterceptor.class);

    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        logger.info("Intercepted onSend: {}", record.toString());
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        logger.info("Broker ACK metadata: {}", metadata);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
