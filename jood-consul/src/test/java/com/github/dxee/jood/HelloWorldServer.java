package com.github.dxee.jood;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

@Singleton
public class HelloWorldServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(HelloWorldServer.class);

    private final ServiceDiscovery serviceDiscovery;
    private final HelloWorldService helloWorldService;

    private Server server;

    @Inject
    public HelloWorldServer(ServiceDiscovery serviceDiscovery, HelloWorldService helloWorldService) {
        this.serviceDiscovery = serviceDiscovery;
        this.helloWorldService = helloWorldService;
    }

    @PostConstruct
    public void start() throws IOException {
        int port = 50051;
        server = ServerBuilder.forPort(port)
                .addService(helloWorldService)
                .build()
                .start();
        LOGGER.info("server started, listening on " + port);

        serviceDiscovery.createService("test","", null,
                "localhost", port, "",
                "localhost:"+port,"5s", "5s");
    }

    @PreDestroy
    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public static class HelloWorldService extends GreeterGrpc.GreeterImplBase {
        @Override
        public void sayHello(HelloRequest request,
                             StreamObserver<HelloReply> responseObserver) {
            String name = request.getName();

            HelloReply response = HelloReply.newBuilder().setMessage("hello " + name).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
