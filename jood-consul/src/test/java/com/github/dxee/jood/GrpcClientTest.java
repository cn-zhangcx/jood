package com.github.dxee.jood;

import com.github.dxee.dject.Dject;
import com.github.dxee.dject.ext.ShutdownHookModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import com.pszymczyk.consul.infrastructure.Ports;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;

public class GrpcClientTest {
    private static ConsulProcess consul;
    private static int randomHttpsPort = Ports.nextAvailable();

    @Inject
    private ServiceDiscovery serviceDiscovery;
    @Inject
    private GrpcLoadBalancer<GreeterGrpc, GreeterGrpc.GreeterBlockingStub, GreeterGrpc.GreeterStub> lb;

    @BeforeClass
    public static void setup() {
        String path = "src/test/resources/ssl";
        String certRootPath = new File(path).getAbsolutePath();
        //language=JSON
        String customConfiguration =
                "{\n" +
                        "  \"datacenter\": \"dc-test\",\n" +
                        "  \"log_level\": \"info\",\n" +
                        "  \"ports\": {\n" +
                        "    \"https\": "+ randomHttpsPort+ "\n" +
                        "  },\n" +
                        "  \"ca_file\": \"" + certRootPath + "/ca.cert\",\n" +
                        "  \"key_file\": \"" + certRootPath + "/key.key\",\n" +
                        "  \"cert_file\": \"" + certRootPath + "/key.crt\"\n" +
                        "}\n";

        consul = ConsulStarterBuilder.consulStarter()
                .withConsulVersion("1.2.2")
                .withCustomConfig(customConfiguration)
                .build()
                .start();

    }

    @AfterClass
    public static void cleanup() {
        consul.close();
    }

    @Test
    public void send_and_recive() {
        String host = "localhost";
        int port = consul.getHttpPort();

        Dject dject = Dject.newBuilder().withModules(new AbstractModule() {
            @Override
            protected void configure() {
                bind(HelloWorldServer.class).asEagerSingleton();
                bind(HelloWorldServer.HelloWorldService.class);
            }

            @Provides
            @Singleton
            public ServiceDiscovery serviceDiscovery() {
                return new ConsulServiceDiscovery(host, port);
            }

            @Provides
            @Singleton
            public GrpcLoadBalancer<GreeterGrpc, GreeterGrpc.GreeterBlockingStub, GreeterGrpc.GreeterStub>
            grpcLoadBalancer(ServiceDiscovery serviceDiscovery) {
                return new GrpcLoadBalancer<>("test", GreeterGrpc.class, serviceDiscovery);
            }
        }, new ShutdownHookModule()).build();

        dject.injectMembers(this);

        String grpcLoadBalancer = "grpc load balancer";
        HelloRequest request = HelloRequest.newBuilder()
                .setName(grpcLoadBalancer)
                .build();

        HelloReply response = lb.getBlockingStub().sayHello(request);
        Assert.assertEquals("hello " + grpcLoadBalancer, response.getMessage());
    }
}