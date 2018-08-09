package com.github.dxee.jood;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.health.model.HealthService;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.ecwid.consul.v1.session.SessionClient;
import com.ecwid.consul.v1.session.SessionConsulClient;
import com.ecwid.consul.v1.session.model.NewSession;
import com.github.dxee.jood.registry.ServiceDiscovery;
import com.github.dxee.jood.registry.ServiceNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;

/**
 * Consul service discovery implements
 *
 * @author bing.fan
 * 2018-08-02 14:49
 */
@Singleton
public class ConsulServiceDiscovery implements ServiceDiscovery {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsulServiceDiscovery.class);

    private ConsulClient client;
    private SessionClient sessionClient;

    @Inject
    public ConsulServiceDiscovery(ConsulFeatures consulFeatures) {
        client = new ConsulClient(consulFeatures.consulHost(), consulFeatures.consulPort());
        sessionClient = new SessionConsulClient(consulFeatures.consulHost(), consulFeatures.consulPort());

        LOGGER.info("consul client info {}:{}", consulFeatures.consulHost(), consulFeatures.consulPort());
    }

    @Override
    public void createService(String serviceName, String id, List<String> tags, String address, int port,
                              String script, String tcp, String interval, String timeout) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(serviceName), "service name must set");
        // register new service with associated health check
        NewService newService = new NewService();
        newService.setName(serviceName);
        newService.setId(id);
        newService.setAddress(address);
        newService.setPort(port);
        if (tags != null) {
            newService.setTags(tags);
        }

        NewService.Check serviceCheck = new NewService.Check();
        if (script != null) {
            serviceCheck.setScript(script);
        }
        if (tcp != null) {
            serviceCheck.setTcp(tcp);
        }
        serviceCheck.setInterval(interval);
        if (timeout != null) {
            serviceCheck.setTimeout(timeout);
        }
        newService.setCheck(serviceCheck);

        client.agentServiceRegister(newService);
    }


    @Override
    public List<ServiceNode> getHealthServices(String serviceName) {
        List<ServiceNode> list = new ArrayList<>();

        Response<List<HealthService>> healthServiceResponse = client.getHealthServices(serviceName,
                true, QueryParams.DEFAULT);

        List<HealthService> healthServices = healthServiceResponse.getValue();
        if (healthServices == null) {
            return list;
        }

        for (HealthService healthService : healthServices) {
            HealthService.Service service = healthService.getService();

            String id = service.getId();
            String address = healthService.getNode().getAddress();
            int port = service.getPort();
            list.add(new ServiceNode(id, address, port));
        }
        return list;
    }

    @Override
    public Map<String, String> getKVValues(String keyPath) {
        Response<List<GetValue>> valueResponse = client.getKVValues(keyPath);
        List<GetValue> getValues = valueResponse.getValue();
        if (getValues == null) {
            return null;
        }

        Map<String, String> map = new HashMap<>();
        for (GetValue v : getValues) {
            if (v == null || v.getValue() == null) {
                continue;
            }
            map.put(v.getKey(), v.getDecodedValue());
        }
        return map;
    }

    @Override
    public String getKVValue(String key) {
        Response<GetValue> valueResponse = client.getKVValue(key);

        GetValue getValue = valueResponse.getValue();
        if (getValue == null) {
            return null;
        }

        return getValue.getDecodedValue();
    }

    @Override
    public Set<String> getKVKeysOnly(String keyPath) {
        Response<List<String>> valueResponse = client.getKVKeysOnly(keyPath);

        List<String> getValues = valueResponse.getValue();
        if (getValues == null) {
            return null;
        }

        Set<String> set = new HashSet<>();
        for (String key : getValues) {
            if (key != null) {
                set.add(key);
            }
        }

        return set;
    }

    @Override
    public void deleteKVValue(String key) {
        client.deleteKVValue(key);
    }

    @Override
    public void deleteKVValuesRecursively(String key) {
        client.deleteKVValues(key);
    }

    @Override
    public Map<String, String> getLeader(String keyPath) {
        Response<List<GetValue>> valueResponse = client.getKVValues(keyPath);

        List<GetValue> getValues = valueResponse.getValue();
        if (getValues == null) {
            return null;
        }

        Map<String, String> map = null;
        int count = 0;
        for (GetValue v : getValues) {
            if (v == null || v.getValue() == null || v.getSession() == null) {
                continue;
            }

            if (count == 0) {
                map = new HashMap<>();
            }
            map.put(v.getKey(), v.getDecodedValue());
            count++;
        }
        return map;
    }


    @Override
    public void setKVValue(String key, String value) {
        client.setKVValue(key, value);
    }

    @Override
    public String createSession(String name, String node, String ttl, long lockDelay) {
        NewSession newSession = new NewSession();
        newSession.setName(name);
        newSession.setNode(node);
        newSession.setTtl(ttl);
        newSession.setLockDelay(lockDelay);
        return sessionClient.sessionCreate(newSession, QueryParams.DEFAULT).getValue();
    }

    @Override
    public void destroySession(String session) {
        sessionClient.sessionDestroy(session, QueryParams.DEFAULT);
    }

    @Override
    public void renewSession(String session) {
        sessionClient.renewSession(session, QueryParams.DEFAULT);
    }

    @Override
    public boolean acquireLock(String key, String value, String session) {
        PutParams putParams = new PutParams();
        putParams.setAcquireSession(session);
        return client.setKVValue(key, value, putParams).getValue();
    }
}
