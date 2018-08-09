package com.github.dxee.jood;

import com.github.dxee.jood.registry.ServiceDiscovery;
import com.github.dxee.jood.registry.ServiceNode;
import com.google.inject.assistedinject.Assisted;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Consul name resolver
 *
 * @author bing.fan
 * 2018-08-08 20:17
 */
public class ConsulNameResolver extends NameResolver {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsulNameResolver.class);

    private final URI uri;
    private final String serviceName;
    private final JoodFeatures joodFeatures;
    private final ServiceDiscovery serviceDiscovery;
    private final ConnectionCheckTimer connectionCheckTimer;

    private Listener listener;
    private List<ServiceNode> nodes;

    @Inject
    public ConsulNameResolver(ServiceDiscovery serviceDiscovery, JoodFeatures joodFeatures,
                              @Assisted URI uri, @Assisted String serviceName) {
        this.joodFeatures = joodFeatures;
        this.serviceDiscovery = serviceDiscovery;
        // run connection check timer.
        connectionCheckTimer = new ConnectionCheckTimer();
        connectionCheckTimer.runTimer();

        this.uri = uri;
        this.serviceName = serviceName;
    }

    @Override
    public String getServiceAuthority() {
        return uri.getAuthority();
    }

    @Override
    public void start(Listener listener) {
        this.listener = listener;
    }

    private void loadServiceNodes() {
        if (null == listener) {
            LOGGER.warn("Listener is null, will not load service nodes from consul");
            return;
        }
        List<EquivalentAddressGroup> addresses = new ArrayList<>();
        nodes = serviceDiscovery.getHealthServices(serviceName);
        if (nodes == null || nodes.size() == 0) {
            LOGGER.info("there is no node info for serviceName: [{}]...", serviceName);
            return;
        }

        String host;
        int port;
        for (ServiceNode node : nodes) {
            host = node.getHost();
            port = node.getPort();
            addresses.add(new EquivalentAddressGroup(new InetSocketAddress(host, port)));
            LOGGER.info("Add address, serviceName: [{}], host: [{}], port: [{}]", serviceName, host, port);
        }

        if (addresses.size() > 0) {
            listener.onAddresses(addresses, Attributes.EMPTY);
        }
    }

    @PreDestroy
    @Override
    public void shutdown() {
        connectionCheckTimer.stopTimer();
    }

    private class ConnectionCheckTimer {
        private final int delay = 1000;
        private final Timer timer;
        private ConnectionCheckTimerTask timerTask;

        public ConnectionCheckTimer() {
            timerTask = new ConnectionCheckTimerTask();
            timer = new Timer();
        }

        public void runTimer() {
            timer.scheduleAtFixedRate(timerTask, delay, joodFeatures.nameresolverPauseInSeconds() * 1000);
        }

        public void stopTimer() {
            timer.cancel();
            LOGGER.info("ConnectionCheckTimer stopped");
        }
    }

    private class ConnectionCheckTimerTask extends TimerTask {
        @Override
        public void run() {
            try {
                if (null == nodes || nodes.isEmpty()) {
                    loadServiceNodes();
                    return;
                }

                for (ServiceNode node : nodes) {
                    String host = node.getHost();
                    int port = node.getPort();
                    try {
                        new Socket(host, port).close();
                    } catch (IOException e) {
                        LOGGER.error("service nodes being reloaded...", e);
                        loadServiceNodes();
                        break;
                    }
                }
            } catch (Exception e) {
                // Exception will cause task terminate, so catch it
                LOGGER.error("ConnectionCheckTimerTask error", e);
            }
        }
    }

}
