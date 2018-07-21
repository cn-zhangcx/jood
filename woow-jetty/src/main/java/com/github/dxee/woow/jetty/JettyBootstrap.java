package com.github.dxee.woow.jetty;

import com.github.dxee.dject.lifecycle.LifecycleListener;
import com.github.dxee.dject.lifecycle.LifecycleShutdown;
import com.github.dxee.woow.jetty.connectors.ConnectorFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Set;

/**
 * JettyBootstrap
 *
 * @author bing.fan
 * 2018-07-08 08:28
 */
@Singleton
public class JettyBootstrap implements LifecycleListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(JettyBootstrap.class);

    private Server server;

    private final JettyFeatures jettyFeatures;
    private final Handler serverHandler;
    private final Set<ConnectorFactory> connectors;
    private final LifecycleShutdown lifecycleShutdown;

    @Inject
    public JettyBootstrap(JettyFeatures jettyFeatures, Handler serverHandler, Set<ConnectorFactory> connectors
            , LifecycleShutdown lifecycleShutdown) {
        this.jettyFeatures = jettyFeatures;
        this.serverHandler = serverHandler;
        this.connectors = connectors;
        this.lifecycleShutdown = lifecycleShutdown;
    }

    @Override
    public void onStarted() {
        server = new Server();
        server.setHandler(serverHandler);
        server.setConnectors(connectors.stream().map(cf -> cf.get(server)).toArray(Connector[]::new));

        ThreadPool threadPool = server.getThreadPool();
        if (threadPool instanceof QueuedThreadPool) {
            ((QueuedThreadPool) threadPool).setMaxThreads(jettyFeatures.maxJettyThreads());
            ((QueuedThreadPool) threadPool).setMinThreads(jettyFeatures.minJettyThreads());
        } else {
            LOGGER.warn("Expected ThreadPool to be instance of QueuedThreadPool, but was {}",
                    server.getThreadPool().getClass().getName());
        }

        startServer();
    }

    public void startServer() {
        try {
            server.start();
            LOGGER.info("Jetty server start successfully");
        } catch (Exception e) {
            LOGGER.error("Jetty server start error", e);
            lifecycleShutdown.shutdown();
        }
    }
    @Override
    public void onStopped(Throwable error) {
        try {
            server.stop();
            LOGGER.info("Jetty server stop successfully");
        } catch (Exception e) {
            LOGGER.error("Jetty server stop error", e);
        }
    }
}
