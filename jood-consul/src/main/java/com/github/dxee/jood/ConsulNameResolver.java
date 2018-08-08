package com.github.dxee.jood;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class ConsulNameResolver extends NameResolver {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsulNameResolver.class);
    private URI uri;
    private String serviceName;
    private int pauseInSeconds;
    private ServiceDiscovery serviceDiscovery;

    private Listener listener;

    private List<ServiceNode> nodes;

    private ConnectionCheckTimer connectionCheckTimer;

    public ConsulNameResolver(URI uri, String serviceName, int pauseInSeconds, ServiceDiscovery serviceDiscovery) {
        this.uri = uri;
        this.serviceName = serviceName;
        this.pauseInSeconds = pauseInSeconds;
        this.serviceDiscovery = serviceDiscovery;
        // run connection check timer.
        connectionCheckTimer = new ConnectionCheckTimer(this.pauseInSeconds);
        connectionCheckTimer.runTimer();
    }

    @Override
    public String getServiceAuthority() {
        return uri.getAuthority();
    }

    @Override
    public void start(Listener listener) {
        this.listener = listener;
        loadServiceNodes();
    }

    private void loadServiceNodes() {
        List<EquivalentAddressGroup> addrs = new ArrayList<>();

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
            List<SocketAddress> sockaddrsList = new ArrayList<SocketAddress>();
            sockaddrsList.add(new InetSocketAddress(host, port));
            addrs.add(new EquivalentAddressGroup(sockaddrsList));

            LOGGER.info("Add addr, serviceName: [{}], host: [{}], port: [{}]", serviceName, host, port);
        }

        if (addrs.size() > 0) {
            listener.onAddresses(addrs, Attributes.EMPTY);
        }
    }

    public List<ServiceNode> getNodes() {
        return nodes;
    }

    @Override
    public void shutdown() {

    }

    private class ConnectionCheckTimer {
        private final int delay = 1000;
        private final int pauseInSeconds;
        private final Timer timer;
        private ConnectionCheckTimerTask timerTask;

        public ConnectionCheckTimer(int pauseInSeconds) {
            this.pauseInSeconds = pauseInSeconds;

            timerTask = new ConnectionCheckTimerTask();
            timer = new Timer();
        }

        public void runTimer() {
            timer.scheduleAtFixedRate(timerTask, delay, pauseInSeconds * 1000);
        }

        public void reset() {
            timerTask.cancel();
            timer.purge();
            timerTask = new ConnectionCheckTimerTask();
        }
    }

    private class ConnectionCheckTimerTask extends TimerTask {
        @Override
        public void run() {
            List<ServiceNode> nodes = getNodes();
            if (null == nodes || nodes.isEmpty()) {
                LOGGER.info("no service nodes...");
                return;
            }

            for (ServiceNode node : nodes) {
                String host = node.getHost();
                int port = node.getPort();
                try {
                    new Socket(host, port);
                } catch (IOException e) {
                    LOGGER.error("service nodes being reloaded...", e);
                    loadServiceNodes();
                    break;
                }
            }
        }
    }

    public static class ConsulNameResolverProvider extends NameResolverProvider {
        private final String serviceName;
        private final int pauseInSeconds;
        private final ServiceDiscovery serviceDiscovery;

        public ConsulNameResolverProvider(String serviceName, int pauseInSeconds, ServiceDiscovery serviceDiscovery) {
            this.serviceName = serviceName;
            this.pauseInSeconds = pauseInSeconds;
            this.serviceDiscovery = serviceDiscovery;
        }

        @Override
        protected boolean isAvailable() {
            return true;
        }

        @Override
        protected int priority() {
            return 5;
        }

        @Nullable
        @Override
        public NameResolver newNameResolver(URI uri, Attributes attributes) {
            return new ConsulNameResolver(uri, serviceName, pauseInSeconds, serviceDiscovery);
        }

        @Override
        public String getDefaultScheme() {
            return "consul";
        }
    }
}
