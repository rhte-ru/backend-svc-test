package com.redhat.dsevosty.common.svc;

import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_JDG_REMOTE_ADDRESS;
import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_JDG_REMOTE_PORT;
import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_OPERATION;

import java.util.UUID;

import com.redhat.dsevosty.common.model.AbstractDataObject;
import com.redhat.dsevosty.common.model.Versionable;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public abstract class DataGridVerticle extends CommonVerticle implements DataGridVerticleMBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataGridVerticle.class);

    private String hotrodServerHost;
    private int hotrodServerPort;

    private RemoteCacheManager manager;
    private RemoteCache<UUID, AbstractDataObject> cache;

    @Override
    public void start(Future<Void> start) {
        Future<Void> cacheStart = Future.<Void>future();
        Future<Void> httpServerStart = Future.<Void>future();
        CompositeFuture.all(httpServerStart, cacheStart).setHandler(ar -> {
            if (ar.succeeded()) {
                start.complete();
                LOGGER.info("All services started");
            } else {
                LOGGER.fatal(ar.cause());
                start.fail(ar.cause());
            }
        });
        super.start(httpServerStart);
        createCacheManagerInFuture(cacheStart);
    }

    protected void createCacheManagerInFuture(Future<Void> start) {
        vertx.<RemoteCacheManager>executeBlocking(future -> {
            RemoteCacheManager rcm = new RemoteCacheManager(getCacheManagerConfiguration());
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                LOGGER.trace("Exception caught while sleep(200) to wait for init {}", e, rcm);
            }
            LOGGER.info("Created RemoteCacheManager={}", rcm);
            future.complete(rcm);
        }, result -> {
            if (result.succeeded()) {
                manager = result.result();
                start.complete();
            } else {
                manager = null;
                LOGGER.fatal("Error while creating remote cache manager", result.cause());
                start.fail(result.cause());
            }
        });
    }

    @Override
    public void stop(Future<Void> stop) {
        LOGGER.info("About to stop Verticle({})", this);
        stopCacheManagerInFuture(stop);
    }

    protected void stopCacheManagerInFuture(Future<Void> stop) {
        if (manager != null) {
            LOGGER.info("About to stop RemoteCacheManager for", serviceContextName);
            manager.stopAsync().whenCompleteAsync((e, ex) -> {
                LOGGER.info("RemoteCacheManager stopped");
                stop.complete();
            });
            manager = null;
        } else {
            stop.complete();
        }
    }
    protected void httpServerStartErrorHandler(Future<Void> future, AsyncResult<HttpServer> result) {
        if (result.succeeded()) {
            LOGGER.info("Vert.x HTTP Server started: " + result.result());
            future.complete();
        } else {
            LOGGER.warn("Error while starting Vert.x HTTP Server", result.cause());
            // future.fail(result.cause());
            future.complete();
        }
    }

    protected synchronized RemoteCache<UUID, AbstractDataObject> getCache() {
        if (cache == null) {
            LOGGER.trace("Trying to get cache: {}", serviceContextName);
            if (manager == null) {
                LOGGER.trace("Remote CacheManager is NULL, reconfigure it first!");
                return null;
            }
            RemoteCache<UUID, AbstractDataObject> rc = manager.getCache(serviceContextName);
            cache = rc;
            LOGGER.trace("Got reference for RemoteCahe={}", rc.getName());
        }
        return cache;
    }

    protected synchronized void resetCache(Throwable t) {
        // if (t instanceof ) {
        cache = null;
        // }
    }

    protected String initConfiguration() {
        JsonObject vertxConfig = config();
        hotrodServerHost = vertxConfig.getString(SERVICE_JDG_REMOTE_ADDRESS.key, SERVICE_JDG_REMOTE_ADDRESS.value);
        hotrodServerPort = Integer
                .valueOf(vertxConfig.getString(SERVICE_JDG_REMOTE_PORT.key, SERVICE_JDG_REMOTE_PORT.value));
        final String info = super.initConfiguration()
                + "INFINISPAN_HOTROD_SERVER_HOST - {}\nINFINISPAN_HOTROD_SERVER_PORT - {}\n";
        return info;
    }

    protected void printInitialConfiguration(String info) {
        LOGGER.info(info, getClass().getName(), getHttpServerHost(), getHttpServerPort(), serviceContextName,
                getEventBusAddress(), hotrodServerHost, hotrodServerPort);
    }

    protected Configuration getCacheManagerConfiguration() {
        LOGGER.debug("Creating remote cache configuration for host={}, port={}", hotrodServerHost, hotrodServerPort);
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.addServer().host(hotrodServerHost).port(hotrodServerPort);
        return builder.build();
    }

    @Override
    protected void defaultEventBusHandler(Message<JsonObject> message) {
        LOGGER.trace("Rise defaultEventBusHandler for message {}", message);
        if (manager == null) {
            replyError(message, "Unable to perform operation, Remote CacheMnager is NULL");
            return;
        }
        MultiMap headers = message.headers();
        String operation = headers.get(SERVICE_OPERATION.key);
        if (operation == null) {
            replyError(message, "Operation must be set at message header " + SERVICE_OPERATION.key);
            return;
        }

        if (customEventBusHandler(message, operation) == false) {
            return;
        }

        if (operation.equalsIgnoreCase("create")) {
            defaultCreateDataObject(message);
            return;
        }
        if (operation.equalsIgnoreCase("get")) {
            defaultGetDataObject(message);
            return;
        }
        if (operation.equalsIgnoreCase("update")) {
            defaultUpdateDataObject(message);
            return;
        }
        if (operation.equalsIgnoreCase("remove")) {
            defaultRemoveDataObject(message);
            return;
        }
        replyError(message, "Unknown operation " + operation);
    }

    protected void getAsyncUtil(UUID id, Message<JsonObject> message, HttpResponseStatus success,
            Handler<JsonObject> replyError) {
        getCache().getAsync(id).whenComplete((fetched, t) -> {
            if (t != null) {
                LOGGER.error("Error occured while working with cache", t);
                replyError(message, t);
                return;
            }
            JsonObject reply = new JsonObject();
            if (fetched == null) {
                replyError.handle(reply);
            } else {
                reply.put("statusCode", success.code());
                LOGGER.trace("GOT just created DataObject: {}", fetched);
                reply.put("result", fetched.toJson());
                LOGGER.trace("Reply to publisher with {}", reply);
            }
            message.reply(reply);
        });
    }

    protected void defaultCreateDataObject(Message<JsonObject> message) {
        JsonObject o = message.body();
        LOGGER.trace("About to PUT into Cache for o={}...", o);

        AbstractDataObject ado = dataObjectFromJson(o);

        if (ado instanceof Versionable) {
            Versionable v = (Versionable) ado;
            if (v.isVersionSet()) {
                replyError(message, "Version of " + Versionable.class.getName() + " must not be set, but got: "
                        + v.versionAsString());
                return;
            }
            v.setVersion();
        }

        final UUID id = ado.getId();

        RemoteCache<UUID, AbstractDataObject> c = getCache();

        if (c.containsKey(id)) {
            replyError(message, "Key '" + id + "' already exists in cache " + c.getName());
            return;
        }

        c.putAsync(id, ado).whenComplete((result, t) -> {
            LOGGER.trace("Cache PUT for id={}  completed with result: {}", id, result);
            if (t != null) {
                LOGGER.error("Error occured while working with cache", t);
                replyError(message, t);
                return;
            }
            getAsyncUtil(id, message, HttpResponseStatus.CREATED, reply -> {
                replyError(message, "Could not get just put dataObject with id " + id);
            });
        });
    }

    protected void defaultGetDataObject(Message<JsonObject> message) {
        final UUID id = UUID.fromString(message.body().getString(HTTP_GET_PARAMETER_ID));
        LOGGER.trace("About to GET Cache for id={}...", id);
        getAsyncUtil(id, message, HttpResponseStatus.OK, reply -> {
            LOGGER.debug("DataObject not found for id={}", id);
            reply.put("statusCode", HttpResponseStatus.NOT_FOUND.code());
        });
    }

    protected void defaultUpdateDataObject(Message<JsonObject> message) {
        JsonObject json = message.body();
        final UUID id = UUID.fromString(json.getString(HTTP_GET_PARAMETER_ID));
        LOGGER.trace("About to UPDATE Cache for id={} for object {}...", id, json);

        AbstractDataObject ado = dataObjectFromJson(json);
        if (ado instanceof Versionable) {
            Versionable _new = (Versionable) ado;
            getCache().getAsync(id).whenComplete((result, t) -> {
                if (result instanceof Versionable) {
                    Versionable _old = (Versionable) result;
                    if (_old.isVersionEqual(_new)) {
                        // real update
                        realDefaultUpdateDataObject(message, ado, true);
                        return;
                    } else {
                        replyError(message, "Update object " + result.toJson() + " with wrong versin "
                                + ado.toJson().getString("version"));
                        return;
                    }
                } else {
                    replyError(message,
                            "Trying to update non-versionable DataObject" + result + " with versionrd " + _new);
                    return;
                }
            });
        } else {
            // Non-versionable
            realDefaultUpdateDataObject(message, ado, false);
        }
    }

    protected void realDefaultUpdateDataObject(Message<JsonObject> message, AbstractDataObject _new,
            boolean versioned) {
        final UUID id = _new.getId();
        AbstractDataObject ado;
        if (versioned) {
            JsonObject json = _new.toJson();
            json.put("version", UUID.randomUUID().toString());
            ado = dataObjectFromJson(json);
        } else {
            ado = _new;
        }

        getCache().replaceAsync(id, ado).whenComplete((result, t) -> {
            LOGGER.trace("Cache REPLACE for id={} completed with result: {}", id, result);
            if (t != null) {
                LOGGER.error("Error occured while working with cache", t);
                replyError(message, t);
                return;
            }
            getAsyncUtil(id, message, HttpResponseStatus.OK, reply -> {
                replyError(message, "Could not get just replaced dataObject with id " + id);
            });
        });
    }

    protected void defaultRemoveDataObject(Message<JsonObject> message) {
        final UUID id = UUID.fromString(message.body().getString(HTTP_GET_PARAMETER_ID));
        LOGGER.debug("About to REMOVE Cache for id={} for object {}...", id);
        getCache().removeAsync(id).whenCompleteAsync((result, t) -> {
            LOGGER.trace("Cache DELETE for id={} completed with result: {}", id, result);
            if (t != null) {
                LOGGER.error("Error occured while working with cache", t);
                replyError(message, t);
                return;
            }
            JsonObject reply = new JsonObject();
            reply.put("statusCode", HttpResponseStatus.NO_CONTENT.code());
            LOGGER.debug("Reply to publisher with {}", reply);
            message.reply(reply);
        });
    }

    private void replyError(Message<JsonObject> message, Throwable t) {
        resetCache(t);
        replyError(message, t.getMessage());
    }

    private void replyError(Message<JsonObject> message, String msg) {
        message.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), msg);
    }

    protected abstract boolean customEventBusHandler(Message<JsonObject> message, String operation);

    protected abstract AbstractDataObject dataObjectFromJson(JsonObject json);

    // Management methods

    @Override
    public void createCacheManager() {
        Future<Void> future = Future.<Void>future();
        future.setHandler(ar -> {
          if (ar.succeeded()) {
            LOGGER.info("HTTP Server has started successfully");
          } else {
            LOGGER.info("Error occured while restarting HTTP Server: ", ar.cause());
          }
        });
        createCacheManagerInFuture(future);
    }
  
    @Override
    public void destroyCacheManger() {
        Future<Void> future = Future.<Void>future();
        future.setHandler(ar -> {
          if (ar.succeeded()) {
            LOGGER.info("HTTP Server has stopped successfully");
          } else {
            LOGGER.info("Error occured while stopping HTTP Server: ", ar.cause());
          }
        });
        stopCacheManagerInFuture(future);
    }
  
    @Override
    public void setHotrodServerHost(String host) {
        hotrodServerHost = host;
    }
  
    @Override
    public String getHotrodServerHost() {
        return hotrodServerHost;
    }
  
    @Override
    public void setHotrodServerPort(int port) {
        hotrodServerPort = port;
    }
  
    @Override
    public int getHotrodServerPort() {
      return hotrodServerPort;
    }
}
