package io.confluent.testcontainers;

import org.testcontainers.utility.DockerImageName;

public class CpKsqlDbServerContainer extends AbstractKsqlDbServerContainer<CpKsqlDbServerContainer> {
    public static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("confluentinc/cp-ksqldb-server");
    public static final String DEFAULT_IMAGE_TAG = "6.2.0-1-ubi8";

    public CpKsqlDbServerContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public CpKsqlDbServerContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
    }
}
