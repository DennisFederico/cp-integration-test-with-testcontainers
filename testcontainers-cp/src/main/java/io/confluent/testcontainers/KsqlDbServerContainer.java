package io.confluent.testcontainers;

import org.testcontainers.utility.DockerImageName;

public class KsqlDbServerContainer extends AbstractKsqlDbServerContainer<KsqlDbServerContainer> {
    public static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("confluentinc/ksqldb-server");
    public static final String DEFAULT_IMAGE_TAG = "0.17.0";

    public KsqlDbServerContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public KsqlDbServerContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
    }
}
