package com.github.dxee.jood;

import java.net.URI;

/**
 * ConsulNameResolverProvider factory
 *
 * @author bing.fan
 * 2018-08-06 18:25
 */
public interface ConsulNameResolverProviderFactory {
    ConsulNameResolverProvider create(String serviceName);

}
