package com.github.dxee.woow.jetty.providers;


import com.github.dxee.woow.jetty.JettyFeatures;
import com.google.inject.servlet.GuiceFilter;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import javax.servlet.DispatcherType;
import java.util.EnumSet;

/**
 * ServletContextHandlerProvider
 *
 * @author bing.fan
 * 2018-07-08 08:33
 */
@Singleton
public class ServletContextHandlerProvider implements Provider<Handler> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServletContextHandlerProvider.class);
    private final JettyFeatures jettyFeatures;

    @Inject
    public ServletContextHandlerProvider(JettyFeatures jettyFeatures) {
        this.jettyFeatures = jettyFeatures;
    }

    @Override
    public Handler get() {
        ServletContextHandler servletContextHandler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        servletContextHandler.setContextPath(jettyFeatures.servletContext());

        servletContextHandler.addFilter(GuiceFilter.class
                , "/*"
                , EnumSet.allOf(DispatcherType.class)
        );

        servletContextHandler.addServlet(DefaultServlet.class, "/");

        LOGGER.info("Init handler and set servlet context to {} successfully", jettyFeatures.servletContext());
        return servletContextHandler;
    }
}
