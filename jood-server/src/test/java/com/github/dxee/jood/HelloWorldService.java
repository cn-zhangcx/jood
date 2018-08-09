package com.github.dxee.jood;

import io.grpc.stub.StreamObserver;

import javax.inject.Singleton;

@Singleton
public class HelloWorldService extends GreeterGrpc.GreeterImplBase {
    @Override
    public void sayHello(HelloRequest request,
                         StreamObserver<HelloReply> responseObserver) {
        String name = request.getName();

        HelloReply response = HelloReply.newBuilder().setMessage("hello " + name).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
