package com.github.dxee.jood;

import com.google.inject.assistedinject.Assisted;
import io.grpc.Attributes;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.net.URI;

public class ConsulNameResolverProvider extends NameResolverProvider {
    private static final String CONSUL = "consul";

    private final String serviceName;
    private final ConsulNameResolverFactory consulNameResolverFactory;

    private NameResolver nameResolver;

    @Inject
    public ConsulNameResolverProvider(ConsulNameResolverFactory consulNameResolverFactory,
                                      @Assisted String serviceName) {
        this.consulNameResolverFactory = consulNameResolverFactory;
        this.serviceName = serviceName;
    }

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 5;
    }

    @Nullable
    @Override
    public NameResolver newNameResolver(URI uri, Attributes attributes) {
        if(null == nameResolver) {
            nameResolver = consulNameResolverFactory.create(uri, serviceName);
        }
        return nameResolver;
    }

    @Override
    public String getDefaultScheme() {
        return CONSUL;
    }
}
