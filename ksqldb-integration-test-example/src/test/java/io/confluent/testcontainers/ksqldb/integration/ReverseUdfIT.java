package io.confluent.testcontainers.ksqldb.integration;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.Row;
import io.confluent.testcontainers.CpKsqlDbServerContainer;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
public class ReverseUdfIT extends AbstractITTestHelper {

    static final String KSQLDB_REQUEST_CONTENT_TYPE = "application/vnd.ksql.v1+json";
    //Built JAR file is available via System.property "udfJarFile" introduced in the failsafe plugin in the pom.xml
    static final String UDF_JAR_FILE = System.getProperty("udfJarFile");
    static final Logger logger = LoggerFactory.getLogger(ReverseUdfIT.class);
    static final Network network = Network.newNetwork();

    @Container
    public final KafkaContainer kafkaContainer = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka")
                    .withTag("6.2.0-1-ubi8"))
            .withReuse(true)
            .withNetwork(network);

//    @Container
//    public final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer(
//            SchemaRegistryContainer.DEFAULT_IMAGE_NAME.withTag(SchemaRegistryContainer.DEFAULT_IMAGE_TAG))
//            .withReuse(true)
//            .withKafka(kafkaContainer);

    @BeforeAll
    static void checkThatTheUdfJarIsAvailable() {
        assertTrue(Files.exists(Path.of(UDF_JAR_FILE)), "The UDF Jar file is not present/packaged");
    }

    @Test
    void checkKsqlServerNodesWithoutAndWithUdfDeployed() {
        try (//UDF Not deployed
             final CpKsqlDbServerContainer ksqlDBWithoutUdf = new CpKsqlDbServerContainer(
                     CpKsqlDbServerContainer.DEFAULT_IMAGE_NAME.withTag(CpKsqlDbServerContainer.DEFAULT_IMAGE_TAG))
                     .withReuse(false)
                     .withLogConsumer(new Slf4jLogConsumer(logger))
                     .withKafkaContainer(kafkaContainer);
             //UDF deployed
             final CpKsqlDbServerContainer ksqlDBWithUdf = new CpKsqlDbServerContainer(
                     CpKsqlDbServerContainer.DEFAULT_IMAGE_NAME.withTag(CpKsqlDbServerContainer.DEFAULT_IMAGE_TAG))
                     .withReuse(false)
                     .withLogConsumer(new Slf4jLogConsumer(logger))
                     .withKafkaContainer(kafkaContainer)
                     .withFileSystemBind(UDF_JAR_FILE, "/etc/ksqldb/ext/udf-test-target.jar", BindMode.READ_ONLY)
                     .withEnv("KSQL_KSQL_EXTENSION_DIR", "/etc/ksqldb/ext")) {

            ksqlDBWithoutUdf.start();
            ksqlDBWithUdf.start();

            //SHOW FUNCTIONS via rest
            String ksqlDbRequest = createKsqlDbJsonRequest("show functions;", null);
            JsonPath responseJsonWithoutUdf = given()
                    .body(ksqlDbRequest)
                    .contentType(KSQLDB_REQUEST_CONTENT_TYPE)
                    .when()
                    .post(ksqlDBWithoutUdf.getKsqlDbUrl() + "/ksql")
                    .then()
                    .contentType(ContentType.JSON)
                    .extract()
                    .response()
                    .jsonPath();

            Map<String, String> noUdfPresent = responseJsonWithoutUdf.getMap("[0].functions.find {it.name=='REVERSE'}");
            assertThat(noUdfPresent, nullValue());

            //SHOW FUNCTIONS via rest
            JsonPath responseJsonWithUdf = given()
                    .body(ksqlDbRequest)
                    .contentType(KSQLDB_REQUEST_CONTENT_TYPE)
                    .when()
                    .post(ksqlDBWithUdf.getKsqlDbUrl() + "/ksql")
                    .then()
                    .contentType(ContentType.JSON)
                    .extract()
                    .response()
                    .jsonPath();

            Map<String, String> udfPresent = responseJsonWithUdf.getMap("[0].functions.find {it.name=='REVERSE'}");
            assertThat(udfPresent, allOf(notNullValue(), hasEntry("type", "SCALAR"), hasEntry("name", "REVERSE")));

        } catch (Exception e) {
            fail(e);
        }
    }

    @ParameterizedTest
    @CsvSource({
            "io/confluent/testcontainers/ksqldb/integration/table/reverseUdfTablePreparation.sql," +
                    "io/confluent/testcontainers/ksqldb/integration/table/reverseUdfTableInputData.json," +
                    "io/confluent/testcontainers/ksqldb/integration/table/reverseUdfTableQueryCheck.sql," +
                    "io/confluent/testcontainers/ksqldb/integration/reverseUdfExpectedResults.txt"
    })
    void reverseUdfOnTableUsingRestClient(String preparationSqlFile, String inputDataFile, String checkQueryFile, String expectedResultsFile) {
        try (final CpKsqlDbServerContainer ksqlDBWithUdf = new CpKsqlDbServerContainer(
                CpKsqlDbServerContainer.DEFAULT_IMAGE_NAME.withTag(CpKsqlDbServerContainer.DEFAULT_IMAGE_TAG))
                .withReuse(false)
                .withLogConsumer(new Slf4jLogConsumer(logger))
                .withKafkaContainer(kafkaContainer)
                .withFileSystemBind(UDF_JAR_FILE, "/etc/ksqldb/ext/udf-test-target.jar", BindMode.READ_ONLY)
                .withEnv("KSQL_KSQL_EXTENSION_DIR", "/etc/ksqldb/ext")) {

            //Start ksqldb
            ksqlDBWithUdf.start();

            //Read preparation statement
            ClassLoader classLoader = getClass().getClassLoader();
            String preparationSql = Files.readString(Path.of(classLoader.getResource(preparationSqlFile).getFile()));

            String[] statements = parseStatements(preparationSql);

            //THROW AN EXCEPTION ON FIRST ERROR
            Arrays.stream(statements).sequential()
                    .map(AbstractITTestHelper::createKsqlDbJsonRequest)
                    .map(s -> given()
                            .body(s)
                            .contentType(KSQLDB_REQUEST_CONTENT_TYPE)
                            .when()
                            .post(ksqlDBWithUdf.getKsqlDbUrl() + "/ksql")
                            .then()
                            .contentType(ContentType.JSON)
                            .extract()
                            .jsonPath()
                    ).filter(jsonPath -> "statement_error".equals(jsonPath.get("@type")))
                    .findFirst()
                    .ifPresent(jsonPath -> fail(String.format("Error processing statement: %s - cause: %s", jsonPath.getString("statementText"), jsonPath.getString("message"))));

            //Insert Test Data
            List<String> inserts = readJsonData(inputDataFile)
                    .map(Map::entrySet)
                    .flatMap(Set::stream)
                    .map(ReverseUdfIT::prepareInsertStatement)
                    .collect(Collectors.toList());

            inserts.stream()
                    .map(AbstractITTestHelper::createKsqlDbJsonRequest)
                    .map(s -> given()
                            .body(s)
                            .contentType(KSQLDB_REQUEST_CONTENT_TYPE)
                            .when()
                            .post(ksqlDBWithUdf.getKsqlDbUrl() + "/ksql")
                            .then()
                            .contentType(ContentType.JSON)
                            .extract()
                            .jsonPath()
                    ).filter(jsonPath -> "statement_error".equals(jsonPath.get("@type")))
                    .findFirst()
                    .ifPresent(jsonPath -> fail(String.format("Error processing statement: %s - cause: %s", jsonPath.getString("statementText"), jsonPath.getString("message"))));

            //Read check query
            String checkQuerySql = Files.readString(Path.of(classLoader.getResource(checkQueryFile).getFile()));
            String queryRequest = createKsqlDbJsonRequest(checkQuerySql, Collections.singletonMap("ksql.streams.auto.offset.reset", "earliest"));

            List<String> reversed = given()
                    .body(queryRequest)
                    .contentType(KSQLDB_REQUEST_CONTENT_TYPE)
                    .when()
                    .post(ksqlDBWithUdf.getKsqlDbUrl() + "/query")
                    .then()
                    .contentType(ContentType.JSON)
                    .statusCode(200)
                    .extract()
                    .jsonPath()
                    .get("row.columns.collect {it[0]}");

            //Assert the expected result
            List<String> expectedResults = Files.readAllLines(Path.of(classLoader.getResource(expectedResultsFile).getFile()));
            assertThat(reversed, contains(expectedResults.toArray()));

        } catch (Exception e) {
            fail(e);
        }
    }

    @ParameterizedTest
    @CsvSource({ //KsqlDb Client does not support INSERT into TABLE yet!
            "io/confluent/testcontainers/ksqldb/integration/stream/reverseUdfStreamPreparation.sql," +
                    "io/confluent/testcontainers/ksqldb/integration/stream/reverseUdfStreamInputData.json," +
                    "io/confluent/testcontainers/ksqldb/integration/table/reverseUdfTableQueryCheck.sql," +
                    "io/confluent/testcontainers/ksqldb/integration/reverseUdfExpectedResults.txt"
    })
    void reverseUdfUsingKsqlDbClient(String preparationSqlFile, String inputDataFile, String checkQueryFile, String expectedResultsFile) {
        try (final CpKsqlDbServerContainer ksqlDBWithUdf = new CpKsqlDbServerContainer(
                CpKsqlDbServerContainer.DEFAULT_IMAGE_NAME.withTag(CpKsqlDbServerContainer.DEFAULT_IMAGE_TAG))
                .withReuse(false)
                .withLogConsumer(new Slf4jLogConsumer(logger))
                .withKafkaContainer(kafkaContainer)
                .withFileSystemBind(UDF_JAR_FILE, "/etc/ksqldb/ext/udf-test-target.jar", BindMode.READ_ONLY)
                .withEnv("KSQL_KSQL_EXTENSION_DIR", "/etc/ksqldb/ext")) {

            //Start ksqldb
            ksqlDBWithUdf.start();

            //Prepare ksqldbClient
            ClientOptions clientOptions = ClientOptions.create()
                    .setHost(ksqlDBWithUdf.getContainerIpAddress())
                    .setPort(ksqlDBWithUdf.getMappedPort(CpKsqlDbServerContainer.DEFAULT_PORT));
            Client client = Client.create(clientOptions);

            //Read preparation statements
            ClassLoader classLoader = getClass().getClassLoader();
            String preparationSql = Files.readString(Path.of(classLoader.getResource(preparationSqlFile).getFile()));

            //Execute preparation statement
            String[] statements = parseStatements(preparationSql);

            Map<String, Object> stmtExecProps = Collections.singletonMap("auto.offset.reset", "earliest");

            Arrays.stream(statements).sequential()
                    .filter(s -> !s.trim().isEmpty())
                    .map(s -> client.executeStatement(s, stmtExecProps))
                    //.map(client::executeStatement)
                    .forEach(CompletableFuture::join);

            //Insert Test Data
            readJsonData(inputDataFile)
                    .map(Map::entrySet)
                    .flatMap(Set::stream)
                    .map(entry -> client.insertInto(entry.getKey(), new KsqlObject(entry.getValue())))
                    .forEach(CompletableFuture::join);

            //Read check query
            String checkQuerySql = Files.readString(Path.of(classLoader.getResource(checkQueryFile).getFile()));

            //Run query
            List<Row> rows = client.executeQuery(checkQuerySql, stmtExecProps).get(30, TimeUnit.SECONDS);

            //Collect the output
            List<String> reversed = rows.stream()
                    .map(row -> row.values().stream().map(Object::toString).collect(Collectors.joining(",")))
                    .collect(Collectors.toList());

            //Assert the expected result
            List<String> expectedResults = Files.readAllLines(Path.of(classLoader.getResource(expectedResultsFile).getFile()));
            assertThat(reversed, contains(expectedResults.toArray()));

        } catch (Exception e) {
            fail(e);
        }
    }
}
