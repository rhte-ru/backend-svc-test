package com.redhat.dsevosty.backend.util;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;

public class InfinispanLocalHotrodServer<K, V> {

    private static final Logger LOGGER = LogManager.getLogger(InfinispanLocalHotrodServer.class);

    private static final String HOTROD_SERVER_HOST = "127.0.0.1";
    private static final int HOTROD_SERVER_PORT = 11222;
    private static final String DEFAULT_CACHE_NAME = "defaultCache";

    // private static UUID k = UUID.randomUUID();

    private EmbeddedCacheManager cm;
    private HotRodServer server;
    private RemoteCache<K, V> cache;
    private RemoteCacheManager rcm;

    public InfinispanLocalHotrodServer() {
        this(new ConfigurationBuilder().build(), new HotRodServerConfigurationBuilder().host(HOTROD_SERVER_HOST)
                .defaultCacheName(DEFAULT_CACHE_NAME).port(HOTROD_SERVER_PORT).build());
    }

    public InfinispanLocalHotrodServer(Configuration cacheConfig, HotRodServerConfiguration serverConfig) {
        cm = new DefaultCacheManager(cacheConfig);
        LOGGER.info("Created CacheManager: " + cm);
        Cache<K, V> localCache = cm.getCache(serverConfig.defaultCacheName());
        LOGGER.debug("There is the Cache: " + localCache);
        server = new HotRodServer();
        server.start(serverConfig, cm);
        LOGGER.info("Started HotRod Server {}", server);
        org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
        builder.addServer().host(server.getHost()).port(server.getPort());
        rcm = new RemoteCacheManager(builder.build());
        cache = rcm.getCache(serverConfig.defaultCacheName());
        LOGGER.info("Connected to RemoteCache={} via RemoteCacheManager={}", cache.getCacheTopologyInfo(), rcm);
    }

    public void initCache(Map<K, V> map) {
        LOGGER.info("Initializing Cache={} with values={}", cache.toString(), map.toString());
        cache.putAll(map);
        // LOGGER.info(cache.get(k));
    }

    public RemoteCache<K, V> getCache() {
        return cache;
    }

    public RemoteCache<K, V> getCache(String name) {
        return rcm.getCache(name);
    }

    public void stop() {
        cache.getRemoteCacheManager().stop();
        server.stop();
        cm.stop();
        LOGGER.info("HotRod Server={} and CacheManager={} stopped", server, cm);
    }

    public String toString() {
        return "HotRod Server=" + server + ", LocalCacheManager=" + cm + ", RemoteCacheManager=" + rcm
                + ", RemoteCache=" + cache;
    }

    // public static void main(String[] args) throws Exception {
    // InfinispanLocalHotrodServer<UUID, SimpleDataObject> server = new
    // InfinispanLocalHotrodServer<UUID, SimpleDataObject>();
    // Map<UUID, SimpleDataObject> map = new HashMap<UUID, SimpleDataObject>();
    // map.put(k, new SimpleDataObject(k, "name 1", k));
    // server.initCache(map);
    // RemoteCache<UUID, SimpleDataObject> cache = server.getCache();
    // LOGGER.info("Looking object: {} for key={}", cache.get(k), k);
    // Thread.sleep(60*1000);
    // server.stop();
    // }
}