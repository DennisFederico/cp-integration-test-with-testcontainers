-- STREAM CREATION
CREATE STREAM text_stream (line_number INT, text VARCHAR)
    WITH (kafka_topic='text_topic', value_format='JSON', partitions='1');

--TABLE CREATION
CREATE TABLE text_table
    AS SELECT line_number, LATEST_BY_OFFSET(text) AS text
    FROM text_stream
    GROUP BY line_number
    EMIT CHANGES;