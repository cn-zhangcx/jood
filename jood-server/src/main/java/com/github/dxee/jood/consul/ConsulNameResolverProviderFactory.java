package com.github.dxee.jood.consul;

/**
 * ConsulNameResolverProvider factory
 *
 * @author bing.fan
 * 2018-08-06 18:25
 */
public interface ConsulNameResolverProviderFactory {
    ConsulNameResolverProvider create(String serviceName);

}
