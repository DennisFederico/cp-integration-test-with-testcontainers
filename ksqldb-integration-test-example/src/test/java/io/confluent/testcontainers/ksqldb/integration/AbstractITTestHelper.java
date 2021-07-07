package io.confluent.testcontainers.ksqldb.integration;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.confluent.ksql.rest.entity.KsqlRequest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class AbstractITTestHelper {
    final static ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module()).enable(JsonParser.Feature.ALLOW_COMMENTS);

    static private final String WITH_DELIMITER = "(?<=%1$s)";
    static private final String INLINE_COMMENTS = "(?m)--.*$";
    static private final String EMPTY_LINES = "(?m)^\\s*$";
    static private final String NEW_LINE = "\n";

    static String[] parseStatements(String sqlStatements) {
        //Client allow only 1 statement to run with each call... we tokenize and split the input file.
        return sqlStatements
                .replaceAll(INLINE_COMMENTS, "")
                .replaceAll(EMPTY_LINES, "")
                .replaceAll(NEW_LINE, "")
                .split(String.format(WITH_DELIMITER, ";"));
    }

    static Stream<Map<String, Map<String, Object>>> readJsonData(String preparationSqlFile) {
        try {

            ClassLoader classLoader = ReverseUdfIT.class.getClassLoader();
            Path path = Path.of(classLoader.getResource(preparationSqlFile).getFile());

            JsonParser parser = mapper.createParser(Files.newBufferedReader(path));
            TypeReference<Map<String, Map<String, Object>>> typeRef = new TypeReference<>() {
            };

            MappingIterator<Map<String, Map<String, Object>>> mappingIterator = mapper.readValues(parser, typeRef);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(mappingIterator, Spliterator.ORDERED), false);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static String prepareInsertStatement(Map.Entry<String, Map<String, Object>> row) {
        List<String> columns = new ArrayList<>();
        List<String> values = new ArrayList<>();
        row.getValue().forEach((key, value) -> {
            columns.add(key);
            if (value instanceof String) {
                values.add(String.format("'%s'", value));
            } else {
                values.add(value.toString());
            }
        });
        return String.format("INSERT INTO %s %s VALUES %s;",
                row.getKey(),
                columns.stream().collect(Collectors.joining(",", "(", ")")),
                values.stream().collect(Collectors.joining(",", "(", ")")));
    }

    static String createKsqlDbJsonRequest(String statement) {
        try {
            return mapper.writeValueAsString(new KsqlRequest(statement, null, null, null));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static String createKsqlDbJsonRequest(final String statement, final Map<String, ?> params) {
        try {
            return mapper.writeValueAsString(new KsqlRequest(statement, null, params, null));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
