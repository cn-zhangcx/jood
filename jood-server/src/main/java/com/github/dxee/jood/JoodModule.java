package com.github.dxee.jood;

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
                .implement(ChannelProvider.class, ChannelProvider.class)
                .build(ChannelProviderFactory.class));

        install(new FactoryModuleBuilder()
                .implement(ConsulNameResolver.class, ConsulNameResolver.class)
                .build(ConsulNameResolverFactory.class));

        install(new FactoryModuleBuilder()
                .implement(ConsulNameResolverProvider.class, ConsulNameResolverProvider.class)
                .build(ConsulNameResolverProviderFactory.class));
    }
}
