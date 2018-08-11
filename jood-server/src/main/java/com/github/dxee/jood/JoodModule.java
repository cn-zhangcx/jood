package com.github.dxee.jood;

import com.github.dxee.jood.channel.GrpcChannel;
import com.github.dxee.jood.channel.GrpcChannelFactory;
import com.github.dxee.jood.consul.ConsulNameResolver;
import com.github.dxee.jood.consul.ConsulNameResolverFactory;
import com.github.dxee.jood.consul.ConsulNameResolverProvider;
import com.github.dxee.jood.consul.ConsulNameResolverProviderFactory;
import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;

/**
 * JoodModule
 *
 * @author bing.fan
 * 2018-08-06 18:47
 */
public class JoodModule extends AbstractModule {

    @Override
    protected void configure() {
        install(new FactoryModuleBuilder()
                .implement(GrpcChannel.class, GrpcChannel.class)
                .build(GrpcChannelFactory.class));

        install(new FactoryModuleBuilder()
                .implement(ConsulNameResolver.class, ConsulNameResolver.class)
                .build(ConsulNameResolverFactory.class));

        install(new FactoryModuleBuilder()
                .implement(ConsulNameResolverProvider.class, ConsulNameResolverProvider.class)
                .build(ConsulNameResolverProviderFactory.class));
    }
}
