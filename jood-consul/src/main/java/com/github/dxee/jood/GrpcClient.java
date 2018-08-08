package com.github.dxee.jood;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

/**
 * Grpc client
 *
 * @author bing.fan
 * 2018-08-02 14:54
 */
public class GrpcClient<R, B, A> {
    private final ManagedChannel channel;
    private B blockingStub;
    private A asyncStub;

    private String host;
    private int port;
    private Class<R> rpcClass;

    public GrpcClient(String host, int port, Class<R> rpcClass) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build(), rpcClass);
        this.host = host;
        this.port = port;
        this.rpcClass = rpcClass;
    }

    @SuppressWarnings("unchecked")
    private GrpcClient(ManagedChannel channel, Class<R> rpcClass) {
        this.channel = channel;

        try {
            Method blockingStubMethod = rpcClass.getMethod("newBlockingStub", Channel.class);
            blockingStub = (B) blockingStubMethod.invoke(null, channel);

            Method asyncStubMethod = rpcClass.getMethod("newStub", Channel.class);
            asyncStub = (A) asyncStubMethod.invoke(null, channel);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("GrpcClient init error", e);
        }
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public B getBlockingStub() {
        return blockingStub;
    }

    public A getAsyncStub() {
        return asyncStub;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Class<R> getRpcClass() {
        return rpcClass;
    }
}
