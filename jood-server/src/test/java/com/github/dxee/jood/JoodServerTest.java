package com.github.dxee.jood;

import com.github.dxee.dject.Dject;
import com.github.dxee.dject.ext.ShutdownHookModule;
import com.github.dxee.jood.registry.ServiceDiscovery;
import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.Multibinder;
import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import com.pszymczyk.consul.infrastructure.Ports;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.inject.Inject;
import java.io.File;

public class JoodServerTest {
    private static ConsulProcess consul;
    private static int randomHttpsPort = Ports.nextAvailable();

    @Inject
    private HelloWorldClient helloWorldClient;

    @BeforeClass
    public static void setup() {
        String path = "src/test/resources/ssl";
        String certRootPath = new File(path).getAbsolutePath();
        //language=JSON
        String customConfiguration =
                "{\n" + "  \"datacenter\": \"dc-test\",\n" + "  \"log_level\": \"info\",\n" + "  \"ports\": {\n"
                        + "    \"https\": " + randomHttpsPort + "\n"
                        + "  },\n"
                        + "  \"ca_file\": \"" + certRootPath + "/ca.cert\",\n"
                        + "  \"key_file\": \"" + certRootPath + "/key.key\",\n"
                        + "  \"cert_file\": \"" + certRootPath + "/key.crt\"\n"
                        + "}\n";

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
    public void send_and_receive() {
        String host = "localhost";
        int port = consul.getHttpPort();

        Dject dject = Dject.newBuilder().withModules(new AbstractModule() {
            @Override
            protected void configure() {
                System.setProperty(ConsulFeatures.CONSUL_SERVER_HOST, host);
                System.setProperty(ConsulFeatures.CONSUL_SERVER_PORT, String.valueOf(port));
                System.setProperty(ConsulFeatures.CONSUL_SERVER_PORT, String.valueOf(port));
                System.setProperty(JoodFeatures.GRPC_SERVICE_NAME, HelloWorldClient.SERVICE_NAME);

                bind(ConsulFeatures.class);
                bind(JoodFeatures.class);
                bind(JoodServer.class).asEagerSingleton();
                bind(ServiceDiscovery.class).to(ConsulServiceDiscovery.class);
                Multibinder<BindableService> serviceBinder = Multibinder.newSetBinder(binder(), BindableService.class);
                serviceBinder.addBinding().to(HelloWorldService.class);
                serviceBinder.addBinding().to(HelloWorldService1.class);

                bind(HelloWorldClient.class);

                install(new JoodModule());
            }
        }, new ShutdownHookModule()).build();

        dject.injectMembers(this);

        String grpcLoadBalancer = "grpc load balancer";
        HelloRequest request = HelloRequest.newBuilder()
                .setName(grpcLoadBalancer)
                .build();
        HelloReply response = helloWorldClient.sayHello(request);

        Assert.assertEquals("hello " + grpcLoadBalancer, response.getMessage());

        request = HelloRequest.newBuilder()
                .setName(grpcLoadBalancer)
                .build();
        response = helloWorldClient.sayHello(request);

        Assert.assertEquals("hello " + grpcLoadBalancer, response.getMessage());
    }
}