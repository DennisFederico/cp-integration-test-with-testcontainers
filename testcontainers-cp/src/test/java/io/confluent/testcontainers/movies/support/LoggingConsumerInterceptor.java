package io.confluent.testcontainers.movies.support;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Map;

public class LoggingConsumerInterceptor implements ConsumerInterceptor {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LoggingConsumerInterceptor.class);

    @Override
    public ConsumerRecords onConsume(ConsumerRecords records) {
        logger.info("Intercepted onConsume: {} records", records.count());
        return records;
    }

    @Override
    public void onCommit(Map offsets) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
