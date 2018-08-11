package com.github.dxee.jood.channel;

import com.github.dxee.jood.ConsulFeatures;
import com.github.dxee.jood.consul.ConsulNameResolverProviderFactory;
import com.google.inject.assistedinject.Assisted;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.util.RoundRobinLoadBalancerFactory;

import javax.inject.Inject;

/**
 * Jood client abstract
 *
 * @author bing.fan
 * 2018-08-02 14:29
 */
public class GrpcChannel {
    private final String serviceName;
    private final ConsulFeatures consulFeatures;
    private final ConsulNameResolverProviderFactory consulNameResolverProviderFactory;

    @Inject
    public GrpcChannel(ConsulNameResolverProviderFactory consulNameResolverProviderFactory,
                       ConsulFeatures consulFeatures,
                       @Assisted String serviceName) {
        this.consulFeatures = consulFeatures;
        this.consulNameResolverProviderFactory = consulNameResolverProviderFactory;
        this.serviceName = serviceName;
    }

    public Channel channel() {
        return ManagedChannelBuilder
                .forTarget("consul://" + consulFeatures.consulHost() + ":" + consulFeatures.consulPort())
                .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
                .nameResolverFactory(consulNameResolverProviderFactory.create(serviceName))
                .usePlaintext(true)
                .build();
    }
}
