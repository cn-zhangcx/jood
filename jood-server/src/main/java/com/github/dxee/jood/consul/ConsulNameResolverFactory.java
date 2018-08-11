package com.github.dxee.jood;

import java.net.URI;

/**
 * Consul name resolver factory
 *
 * @author bing.fan
 * 2018-08-03 15:27
 */
public interface ConsulNameResolverFactory {
    ConsulNameResolver create(URI uri, String serviceName);
}
