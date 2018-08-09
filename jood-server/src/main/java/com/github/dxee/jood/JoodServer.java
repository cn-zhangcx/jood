package com.github.dxee.jood;

import com.github.dxee.dject.lifecycle.LifecycleListener;
import com.github.dxee.dject.lifecycle.LifecycleShutdown;
import com.github.dxee.jood.registry.ServiceDiscovery;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Jood server
 *
 * @author bing.fan
 * 2018-08-11 11:29
 */
@Singleton
public class JoodServer implements LifecycleListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(JoodServer.class);

    private final JoodFeatures joodFeatures;
    private final ConsulFeatures consulFeatures;
    private final Set<BindableService> services;
    private final ServiceDiscovery serviceDiscovery;
    private final LifecycleShutdown lifecycleShutdown;

    private Server server;

    @Inject
    public JoodServer(Set<BindableService> services, ServiceDiscovery serviceDiscovery,
                      LifecycleShutdown lifecycleShutdown, JoodFeatures joodFeatures, ConsulFeatures consulFeatures) {
        this.joodFeatures = joodFeatures;
        this.consulFeatures = consulFeatures;
        this.services = services;
        this.serviceDiscovery = serviceDiscovery;
        this.lifecycleShutdown = lifecycleShutdown;
    }

    @Override
    public void onStarted() {
        try {
            ServerBuilder serverBuilder = NettyServerBuilder.forAddress(
                    new InetSocketAddress(joodFeatures.grpcHost(), joodFeatures.grpcPort())
            );
            services.forEach((service) -> serverBuilder.addService(service));
            server = serverBuilder.build().start();
            // service registry
            consulRegistry();
            LOGGER.info("Grpc NettyServer started, listening on {}", joodFeatures.grpcPort());
        } catch (Exception e) {
            LOGGER.error("Grpc NettyServer start failed", e);
            lifecycleShutdown.shutdown();
        }
    }

    private void consulRegistry() {
        serviceDiscovery.createService(
                joodFeatures.grpcServiceName(),
                "", null,
                joodFeatures.grpcHost(), joodFeatures.grpcPort(),
                "",
                joodFeatures.grpcHost() + ":" + joodFeatures.grpcPort(),
                consulFeatures.consulServiceCheckInterval(), consulFeatures.consulServiceCheckTimeout()
        );
    }

    @Override
    public void onStopped(Throwable error) {
        if (server != null) {
            server.shutdown();
        }
    }
}
