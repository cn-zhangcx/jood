package com.github.dxee.joo.jetty;

import com.github.dxee.dject.spi.PropertySource;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * JettyFeatures
 *
 * @author bing.fan
 * 2018-07-09 09:09
 */
@Singleton
public class JettyFeatures {
    /**
     * For Connector configuration
     **/
    public static final String JETTY_CONNECTOR_HOST = "joo.jetty.connector.host";
    public static final String JETTY_CONNECTOR_HTTP_PORT = "joo.jetty.connector.http.port";

    /**
     * For jettry server
     **/
    public static final String JETTY_SERVER_MAXJETTYTHREADS = "joo.jetty.server.maxjettythreads";
    public static final String JETTY_SERVER_MINJETTYTHREADS = "joo.jetty.server.minjettythreads";

    public static final String JETTY_SERVLET_CONTEXT = "joo.jetty.servlet.context";

    /**
     * For HttpConfiguration
     */
    public static final String JETTY_HTTPCONFIGURATION_SENDXPOWEREDBY =
            "joo.jetty.httpconfiguration.sendxpoweredby";
    public static final String JETTY_HTTPCONFIGURATION_SENDSERVERVERSION =
            "joo.jetty.httpconfiguration.sendserverversion";


    private final PropertySource propertySource;

    @Inject
    public JettyFeatures(PropertySource propertySource) {
        this.propertySource = propertySource;
    }

    /**
     * For Connector configuration
     **/

    public String host() {
        return propertySource.get(JETTY_CONNECTOR_HOST, "127.0.0.1");
    }

    public int httpPort() {
        return propertySource.get(JETTY_CONNECTOR_HTTP_PORT, Integer.class, 8080);
    }

    /**
     * For jetty server
     */
    public int maxJettyThreads() {
        return propertySource.get(JETTY_SERVER_MAXJETTYTHREADS, Integer.class, 64);
    }

    public int minJettyThreads() {
        return propertySource.get(JETTY_SERVER_MINJETTYTHREADS, Integer.class, 2);
    }

    /**
     * For servlet
     */
    /** Servlet context. eg: "/", "/helloword/" **/
    public String servletContext() {
        return propertySource.get(JETTY_SERVLET_CONTEXT, "/");
    }

    /**
     * For HttpConfiguration
     **/
    public boolean sendXPoweredBy() {
        return propertySource.get(JETTY_HTTPCONFIGURATION_SENDXPOWEREDBY,
                Boolean.class, false);
    }

    public boolean sendServerVersion() {
        return propertySource.get(JETTY_HTTPCONFIGURATION_SENDSERVERVERSION,
                Boolean.class, false);
    }
}
