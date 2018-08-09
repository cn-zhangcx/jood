package com.github.dxee.jood;

import io.grpc.stub.StreamObserver;

import javax.inject.Singleton;

@Singleton
public class HelloWorldService1 extends Greeter1Grpc.Greeter1ImplBase {
    @Override
    public void sayHello(HelloRequest request,
                         StreamObserver<HelloReply> responseObserver) {
        String name = request.getName();

        HelloReply response = HelloReply.newBuilder().setMessage("hi " + name).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
