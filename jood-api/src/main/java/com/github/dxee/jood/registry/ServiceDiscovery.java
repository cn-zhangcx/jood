package com.github.dxee.jood.registry;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ServiceDiscovery
 *
 * @author bing.fan
 * 2018-08-02 14:44
 */
public interface ServiceDiscovery {

    /**
     * create service to consul
     * @param serviceName
     * @param id
     * @param tags
     * @param address
     * @param port
     * @param script
     * @param tcp         "localhost:9911"
     * @param interval    "10s"
     * @param timeout     "1s"
     */
    void createService(String serviceName, String id, List<String> tags, String address, int port, String script,
                       String tcp, String interval, String timeout);

    List<ServiceNode> getHealthServices(String path);

    Set<String> getKVKeysOnly(String keyPath);

    String getKVValue(String key);

    Map<String, String> getKVValues(String keyPath);

    Map<String, String> getLeader(String keyPath);

    void setKVValue(String key, String value);

    void deleteKVValue(String key);

    void deleteKVValuesRecursively(String key);

    String createSession(String name, String node, String ttl, long lockDelay);

    void renewSession(String session);

    boolean acquireLock(String key, String value, String session);

    void destroySession(String session);

}
