package com.github.dxee.woow.jetty;

import com.github.dxee.dject.Dject;
import com.github.dxee.dject.ext.ShutdownHookModule;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.eclipse.jetty.util.Jetty;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * JettyModuleTest
 *
 * @author bing.fan
 * 2018-07-11 11:55
 */
public class JettyModuleTest {

    private static final String HELLO = "Hello ";

    private int port;
    private String servletContext = "/jetty/";

    @Singleton
    public static class HelloServlet extends HttpServlet {
        private HelloService helloService;

        @Inject
        public void injectme(HelloService helloService) {
            this.helloService = helloService;
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            resp.getWriter().print(helloService.hello(req.getParameter("name")));
        }
    }

    @Singleton
    public static class HelloService {
        public String hello(String name) {
            return HELLO + name;
        }
    }

    @Before
    public void before() {
        port = TestUtils.freePort();
        System.setProperty(JettyFeatures.JETTY_CONNECTOR_HTTP_PORT, String.valueOf(port));
        System.setProperty(JettyFeatures.JETTY_SERVLET_CONTEXT, servletContext);
    }

    @Test
    public void requestOk() throws InterruptedException {
        Dject.builder().withModules(new JettyModule() {
            @Override
            protected void bindServlets() {
                serve("/hello").with(HelloServlet.class);
            }
        }, new AbstractModule() {
            @Override
            protected void configure() {
                bind(HelloService.class);
            }
        }, new ShutdownHookModule()).build();

        given().port(port).param("name", "World!")
                .when().get("/jetty/hello")
                .then().body(equalTo(HELLO + "World!"));
    }

    @Test
    public void request404() throws InterruptedException {
        Dject.builder().withModules(new JettyModule() {
            @Override
            protected void bindServlets() {
            }
        }, new ShutdownHookModule()).build();

        given().port(port)
                .when().get("/jetty/hello")
                .then().body(containsString("HTTP ERROR 404"));
    }

    @Test
    public void request() throws InterruptedException {
        Dject.builder().withModules(new JettyModule() {
            @Override
            protected void bindServlets() {
            }
        }, new ShutdownHookModule()).build();

        given().port(port)
                .when().get("/jetty/")
                .then().body(containsString("HTTP ERROR 404"));
    }

}