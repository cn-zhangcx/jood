package com.github.dxee.jood;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.IOException;

public class HelloWorldServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(HelloWorldServer.class);

    private final HelloWorldService helloWorldService;

    private Server server;

    @Inject
    public HelloWorldServer(HelloWorldService helloWorldService) {
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
    }

    @PreDestroy
    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public static class HelloWorldService extends GreeterGrpc.GreeterImplBase {
        private final ServiceDiscovery serviceDiscovery;

        @Inject
        public HelloWorldService(ServiceDiscovery serviceDiscovery) {
            this.serviceDiscovery = serviceDiscovery;
        }

        @PostConstruct
        public void postConstruct() {
            serviceDiscovery.createService("test","", null,
                    "localhost", 50051, "",
                    "localhost:50051","5s", "5s");
        }

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
