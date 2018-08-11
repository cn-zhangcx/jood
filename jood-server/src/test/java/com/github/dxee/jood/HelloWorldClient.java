package com.github.dxee.jood;

import com.github.dxee.jood.channel.GrpcChannelFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class HelloWorldClient extends JoodClient {
    public static final String SERVICE_NAME = "hello world";
    private final GreeterGrpc.GreeterBlockingStub blockingStub;
    private final Greeter1Grpc.Greeter1BlockingStub blockingStub1;

    @Inject
    public HelloWorldClient(GrpcChannelFactory grpcChannelFactory) {
        super(SERVICE_NAME, grpcChannelFactory);
        blockingStub = GreeterGrpc.newBlockingStub(channel);
        blockingStub1 = Greeter1Grpc.newBlockingStub(channel);
    }

    public HelloReply sayHello(HelloRequest request) {
        HelloReply response = blockingStub.sayHello(request);
        return response;
    }

    public HelloReply sayHello1(HelloRequest request) {
        HelloReply response = blockingStub1.sayHello(request);
        return response;
    }
}
