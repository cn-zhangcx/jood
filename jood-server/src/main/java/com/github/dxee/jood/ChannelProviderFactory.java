package com.github.dxee.jood;

/**
 * ChannelProvider factory
 *
 * @author bing.fan
 * 2018-08-06 18:29
 */
public interface ChannelProviderFactory {
    ChannelProvider create(String serviceName);
}
