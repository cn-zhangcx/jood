package com.github.dxee.jood;

import com.google.common.base.MoreObjects;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Encapsulates context information needed for services to communicate, share
 * correlation IDs, get information about the original request, gives the
 * ability to pass arbitrary information to services in the service call chain, etc.
 *
 * @author bing.fan
 * 2018-07-10 10:35
 */
public class JoodContext {
    private static final String RPC_ORIGIN_SERVICE = "X-Sx-From-Service";
    private static final String RPC_ORIGIN_METHOD = "X-Sx-From-Method";

    private String correlationId;
    private Map<String, String> properties = new HashMap<>();

    public JoodContext() {
        this(null, null);
    }

    public JoodContext(Map<String, String> properties) {
        this(properties == null ? null : properties.remove("x-correlation-id"), properties);
    }

    public JoodContext(String correlationId, Map<String, String> properties) {
        if (correlationId == null) {
            this.correlationId = UUID.randomUUID().toString();
        } else {
            this.correlationId = correlationId;
        }
        if (properties != null) {
            this.properties = properties;
        }
    }

    public JoodContext(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        if (correlationId != null && this.correlationId == null) {
            this.correlationId = correlationId;
        }
    }

    public String getRpcOriginService() {
        return getProperty(RPC_ORIGIN_SERVICE);
    }

    public String getRpcOriginMethod() {
        return getProperty(RPC_ORIGIN_METHOD);
    }

    public void setProperty(String key, String value) {
        properties.put(key.toLowerCase(), value);
    }

    public Map<String, String> getProperties() {
        return new HashMap<>(properties);
    }

    public String getProperty(String key) {
        return properties.get(key.toLowerCase());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("correlationId", correlationId)
                .add("properties", properties)
                .toString();
    }
}
