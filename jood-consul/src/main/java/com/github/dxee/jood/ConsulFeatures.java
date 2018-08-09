package com.github.dxee.jood;

import com.github.dxee.dject.spi.PropertySource;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Consul features
 *
 * @author bing.fan
 * 2018-08-01 13:48
 */
@Singleton
public class ConsulFeatures {
    /**
     * For consul server
     **/
    public static final String CONSUL_SERVER_HOST = "jood.consul.host";
    public static final String CONSUL_SERVER_PORT = "jood.consul.port";
    public static final String CONSUL_SERVICECHECK_INTERVAL = "jood.consul.servicecheck.interval";
    public static final String CONSUL_SERVICECHECK_TIMEOUT = "jood.consul.servicecheck.timeout";

    private final PropertySource propertySource;

    @Inject
    public ConsulFeatures(PropertySource propertySource) {
        this.propertySource = propertySource;
    }

    /**
     * eg: "localhost", "127.0.0.1"
     */
    public String consulHost() {
        return propertySource.get(CONSUL_SERVER_HOST);
    }

    /**
     * eg: 8080
     */
    public int consulPort() {
        return propertySource.get(CONSUL_SERVER_PORT, Integer.class);
    }

    /**
     * default: 5s
     */
    public String consulServiceCheckInterval() {
        return propertySource.get(CONSUL_SERVICECHECK_INTERVAL, "5s");
    }

    /*
     * default: 1s
     */
    public String consulServiceCheckTimeout() {
        return propertySource.get(CONSUL_SERVICECHECK_TIMEOUT, "1s");
    }
}
