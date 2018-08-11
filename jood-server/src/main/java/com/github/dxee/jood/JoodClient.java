package com.github.dxee.jood;

import com.github.dxee.jood.channel.GrpcChannelFactory;
import io.grpc.Channel;

/**
 * JoodClient
 *
 * @author bing.fan
 * 2018-08-06 18:31
 */
public abstract class JoodClient {
    protected final String serviceName;
    protected final GrpcChannelFactory grpcChannelFactory;
    protected final Channel channel;

    public JoodClient(String serviceName, GrpcChannelFactory grpcChannelFactory) {
        this.serviceName = serviceName;
        this.grpcChannelFactory = grpcChannelFactory;
        this.channel = grpcChannelFactory.create(serviceName).channel();
    }
}
