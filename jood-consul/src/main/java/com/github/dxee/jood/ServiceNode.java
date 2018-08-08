package com.github.dxee.jood;

public class ServiceNode {
    private String id;
    private String host;
    private int port;

    public ServiceNode(String id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
