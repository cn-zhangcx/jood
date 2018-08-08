package com.github.dxee.jood;

import com.github.dxee.dject.lifecycle.LifecycleListener;
import com.github.dxee.jood.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Grpc load balancer
 *
 * @author bing.fan
 * 2018-08-02 14:56
 */
public class GrpcLoadBalancer<R, B, A> implements LifecycleListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcLoadBalancer.class);
    private static final int DEFAULT_PAUSE_IN_SECONDS = 5;

    private RoundRobin<GrpcClient<R, B, A>> roundRobin;
    private List<RoundRobin.Robin<GrpcClient<R, B, A>>> robinList;

    private ReentrantLock lock = new ReentrantLock();

    private ServiceDiscovery serviceDiscovery;
    private String serviceName;
    private Class<R> rpcClass;
    private int pauseInSeconds;

    private ConnectionCheckTimer connectionCheckTimer;

    /**
     * using consul service discovery.
     */
    public GrpcLoadBalancer(String serviceName, Class<R> rpcClass, ServiceDiscovery serviceDiscovery) {
        this(serviceName, rpcClass, DEFAULT_PAUSE_IN_SECONDS, serviceDiscovery);
    }

    public GrpcLoadBalancer(String serviceName, Class<R> rpcClass, int pauseInSeconds,
                            ServiceDiscovery serviceDiscovery) {
        this.serviceDiscovery = serviceDiscovery;
        this.serviceName = serviceName;
        this.rpcClass = rpcClass;
        this.pauseInSeconds = pauseInSeconds;
        // run connection check timer.
        this.connectionCheckTimer = new ConnectionCheckTimer(this.pauseInSeconds);
    }

    public B getBlockingStub() {
        lock.lock();
        try {
            return roundRobin.next().getBlockingStub();
        } finally {
            lock.unlock();
        }
    }

    public A getAsyncStub() {
        lock.lock();
        try {
            return roundRobin.next().getAsyncStub();
        } finally {
            lock.unlock();
        }
    }

    public List<RoundRobin.Robin<GrpcClient<R, B, A>>> getRobinList() {
        return robinList;
    }

    @Override
    public void onStarted() {
        loadServiceNodes();

        connectionCheckTimer.runTimer();
    }

    @Override
    public void onStopped(Throwable error) {
        for (RoundRobin.Robin<GrpcClient<R, B, A>> robin : robinList) {
            try {
                robin.call().shutdown();
            } catch (InterruptedException e) {
                LOGGER.error("robin shutdown interrupted", e);
            }
        }
    }

    private void loadServiceNodes() {
        lock.lock();
        try {
            robinList = new ArrayList<>();
            List<ServiceNode> nodes = null;
            while (nodes == null || nodes.isEmpty()) {
                nodes = serviceDiscovery.getHealthServices(serviceName);
                LOGGER.info("there is no node info for serviceName: [{}], sleep[{}s]...", serviceName, pauseInSeconds);
                TimeUtils.sleep(pauseInSeconds * 1000);
            }

            String host;
            int port = -1;
            for (ServiceNode node : nodes) {
                host = node.getHost();
                port = node.getPort();
                GrpcClient<R, B, A> client = new GrpcClient<>(host, port, rpcClass);
                robinList.add(new RoundRobin.Robin<>(client));
                LOGGER.info("Add service, serviceName: [{}], host: [{}], port: [{}]", serviceName, host, port);
            }
            roundRobin = new RoundRobin<>(robinList);
        } finally {
            lock.unlock();
        }
    }

    private class ConnectionCheckTimer {
        private ConnectionCheckTimerTask timerTask;
        private int delay = 1000;
        private int pauseInSeconds;
        private Timer timer;

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
            for (RoundRobin.Robin<GrpcClient<R, B, A>> robin : getRobinList()) {
                try {
                    new Socket(robin.call().getHost(), robin.call().getPort());
                } catch (IOException e) {
                    LOGGER.error("service nodes being reloaded, {}", e.getMessage());
                    loadServiceNodes();
                    break;
                }
            }
        }
    }
}
