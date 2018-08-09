package com.github.dxee.jood;

import io.grpc.Channel;

/**
 * JoodClient
 *
 * @author bing.fan
 * 2018-08-06 18:31
 */
public abstract class JoodClient {
    protected final String serviceName;
    protected final ChannelProviderFactory channelProviderFactory;
    protected final Channel channel;

    public JoodClient(String serviceName, ChannelProviderFactory channelProviderFactory) {
        this.serviceName = serviceName;
        this.channelProviderFactory = channelProviderFactory;
        this.channel = channelProviderFactory.create(serviceName).channel();
    }
}
