
--TABLE CREATION    MORE
CREATE TABLE text_table (
        line_number INT PRIMARY KEY, --THE ID OR LINE NUMBER    AFTER TAB
        text VARCHAR
    ) WITH (
        kafka_topic='text_topic',
        --IT IS IMPORTANT TO SET THE NUMBER OF PARTITIONS
        partitions='1', FORMAT = 'JSON'
    );

-- INSERTS CAN BE PLACES HERE TOO, BUT ONLY FOR INSERT WITH REST-CLIENT
-- INSERT INTO text (line_number, text) VALUES (1, 'hello'); INSERT INTO text (line_number, text) VALUES (2, 'world');
--
-- INSERT INTO text (line_number, text) VALUES (3, 'this')
-- ;
-- INSERT INTO text (line_number, text) VALUES (4, 'is');
--
-- INSERT INTO text (line_number, text) VALUES (5, 'kafka'); INSERT INTO text (line_number, text) VALUES (6, 'and ksqldb on fire');

