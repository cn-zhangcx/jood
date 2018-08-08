package com.github.dxee.jood;

/**
 * ServiceNode
 *
 * @author bing.fan
 * 2018-08-08 20:17
 */
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
