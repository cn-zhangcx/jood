package com.github.dxee.woow.jetty;

import com.github.dxee.woow.jetty.connectors.ConnectorFactory;
import com.github.dxee.woow.jetty.connectors.HttpConnectorFactory;
import com.github.dxee.woow.jetty.providers.ServletContextHandlerProvider;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.servlet.ServletModule;
import org.eclipse.jetty.server.Handler;

/**
 * Code Comment Here
 *
 * @author bing.fan
 * 2018-07-10 10:38
 */
public abstract class JettyModule extends ServletModule {
    @Override
    protected final void configureServlets() {
        bind(JettyFeatures.class);
        Multibinder<ConnectorFactory> connectorBinder = Multibinder.newSetBinder(binder()
                , ConnectorFactory.class);
        connectorBinder.addBinding().to(HttpConnectorFactory.class);
        bind(Handler.class).toProvider(ServletContextHandlerProvider.class);
        bind(JettyBootstrap.class).asEagerSingleton();

        bindServlets();
    }

    protected abstract void bindServlets();
}
