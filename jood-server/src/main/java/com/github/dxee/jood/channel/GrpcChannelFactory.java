package com.github.dxee.jood.channel;

/**
 * GrpcChannel factory
 *
 * @author bing.fan
 * 2018-08-06 18:29
 */
public interface GrpcChannelFactory {
    GrpcChannel create(String serviceName);
}
