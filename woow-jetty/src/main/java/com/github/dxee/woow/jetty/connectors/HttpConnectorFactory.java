package com.github.dxee.woow.jetty.connectors;

import com.github.dxee.woow.jetty.JettyFeatures;
import org.eclipse.jetty.http.HttpScheme;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * HttpConnectorFactory
 *
 * @author bing.fan
 * 2018-07-08 08:46
 */
@Singleton
public class HttpConnectorFactory implements ConnectorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpConnectorFactory.class);
    private final JettyFeatures jettyFeatures;

    @Inject
    public HttpConnectorFactory(JettyFeatures jettyFeatures) {
        this.jettyFeatures = jettyFeatures;
    }


    @Override
    public ServerConnector get(Server server) {
        HttpConfiguration configuration = new HttpConfiguration();
        configuration.setSecureScheme(HttpScheme.HTTPS.asString());
        configuration.setSecurePort(jettyFeatures.httpPort());
        configuration.setSendXPoweredBy(jettyFeatures.sendXPoweredBy());
        configuration.setSendServerVersion(jettyFeatures.sendServerVersion());

        final ServerConnector connector = new ServerConnector(server,
                new HttpConnectionFactory(configuration));
        connector.setPort(jettyFeatures.httpPort());
        connector.setHost(jettyFeatures.host());

        LOGGER.info("Create server connector {}:{} successfully", jettyFeatures.host(), jettyFeatures.httpPort());
        return connector;
    }
}
