package com.github.dxee.jood.registry;

import java.util.List;

/**
 * Service define for service discovery
 *
 * @author bing.fan
 * 2018-08-01 13:27
 */
public class ServiceDefine {
    private final String serviceName;
    private final String id;
    private final List<String> tags;
    private final String address;
    private final int port;
    private final String script;
    // eg: "localhost:9911"
    private final String tcp;
    // eg: "10s"
    private final String interval;
    // eg: "1s"
    private final String timeout;

    private ServiceDefine(Builder builder) {
        serviceName = builder.serviceName;
        id = builder.id;
        tags = builder.tags;
        address = builder.address;
        port = builder.port;
        script = builder.script;
        tcp = builder.tcp;
        interval = builder.interval;
        timeout = builder.timeout;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getId() {
        return id;
    }

    public List<String> getTags() {
        return tags;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public String getScript() {
        return script;
    }

    public String getTcp() {
        return tcp;
    }

    public String getInterval() {
        return interval;
    }

    public String getTimeout() {
        return timeout;
    }


    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String serviceName;
        private String id;
        private List<String> tags;
        private String address;
        private int port;
        private String script;
        private String tcp;
        private String interval;
        private String timeout;

        private Builder() {
        }

        public Builder withServiceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withTags(List<String> tags) {
            this.tags = tags;
            return this;
        }

        public Builder withAddress(String address) {
            this.address = address;
            return this;
        }

        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public Builder withScript(String script) {
            this.script = script;
            return this;
        }

        public Builder withTcp(String tcp) {
            this.tcp = tcp;
            return this;
        }

        public Builder withInterval(String interval) {
            this.interval = interval;
            return this;
        }

        public Builder withTimeout(String timeout) {
            this.timeout = timeout;
            return this;
        }

        public ServiceDefine build() {
            return new ServiceDefine(this);
        }
    }
}
