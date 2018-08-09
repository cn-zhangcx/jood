package com.github.dxee.jood;

import com.github.dxee.dject.spi.PropertySource;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * JoodFeatures
 *
 * @author bing.fan
 * 2018-07-09 09:09
 */
@Singleton
public class JoodFeatures {
    /**
     * For grpc server
     **/
    public static final String GRPC_SERVER_HOST = "jood.grpc.server.host";
    public static final String GRPC_SERVER_PORT = "jood.grpc.server.port";
    public static final String GRPC_SERVICE_NAME = "jood.grpc.service.name";
    public static final String GRPC_NAMERESOLVER_PAUSEINSECONDS = "jood.consul.nameresolver.pauseinseconds";

    private final PropertySource propertySource;

    @Inject
    public JoodFeatures(PropertySource propertySource) {
        this.propertySource = propertySource;
    }

    /**
     * For Connector configuration
     */
    public String grpcHost() {
        return propertySource.get(GRPC_SERVER_HOST, "127.0.0.1");
    }

    public int grpcPort() {
        return propertySource.get(GRPC_SERVER_PORT, Integer.class, 8080);
    }

    public String grpcServiceName() {
        return propertySource.get(GRPC_SERVICE_NAME);
    }

    /**
     * eg: 8080
     */
    public int nameresolverPauseInSeconds() {
        return propertySource.get(GRPC_NAMERESOLVER_PAUSEINSECONDS, Integer.class, 5);
    }
}
