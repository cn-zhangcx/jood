package com.github.dxee.joo.jetty.connectors;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

/**
 * ConnectorFactory
 *
 * @author bing.fan
 * 2018-07-08 08:28
 */
public interface ConnectorFactory {

    /**
     * Creates {@link ServerConnector} relates to provided {@link Server}
     *
     * @param server Jetty Server
     * @return ServerConnector
     */
    ServerConnector get(Server server);
}
