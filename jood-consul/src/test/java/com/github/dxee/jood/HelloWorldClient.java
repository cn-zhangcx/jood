package com.github.dxee.jood;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.util.RoundRobinLoadBalancerFactory;

public class HelloWorldClient {
    private final ManagedChannel channel;
    private GreeterGrpc.GreeterBlockingStub blockingStub;

    public HelloWorldClient(String serviceName,
                            String consulHost,
                            int consulPort,
                            ServiceDiscovery serviceDiscovery) {

        String consulAddr = "consul://" + consulHost + ":" + consulPort;
        channel = ManagedChannelBuilder
                .forTarget(consulAddr)
                .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
                .nameResolverFactory(new ConsulNameResolver.ConsulNameResolverProvider(serviceName,
                        5, serviceDiscovery))
                .usePlaintext(true)
                .build();

        blockingStub = GreeterGrpc.newBlockingStub(channel);
    }

    public HelloReply sayHello(HelloRequest request) {
        HelloReply response = blockingStub.sayHello(request);
        return response;
    }
}
