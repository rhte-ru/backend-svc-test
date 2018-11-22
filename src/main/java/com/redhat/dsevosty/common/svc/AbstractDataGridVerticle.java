package com.redhat.dsevosty.common.svc;

import static com.redhat.dsevosty.common.ServiceConstant.*;

import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.redhat.dsevosty.common.model.AbstractDataObject;
import com.redhat.dsevosty.common.model.Versionable;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

public abstract class AbstractDataGridVerticle extends AbstractVerticle implements AbstractDataGridVerticleMBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDataGridVerticle.class);

    private static final String HTTP_GET_PARAMETER_ID = "id";

    protected String serviceContextName;
    protected String eventBusAddress;

    private String hotrodServerHost;
    private int hotrodServerPort;

    private String httpServerHost;
    private int httpServerPort;

    private RemoteCacheManager manager;
    private RemoteCache<UUID, AbstractDataObject> cache;

    private EventBus eb;

    protected Router rootRouter;

    @Override
    public void stop(Future<Void> stop) {
        LOGGER.trace("About to stop Verticle");
        if (manager != null) {
            manager.stopAsync().whenCompleteAsync((e, ex) -> {
                stop.complete();
            });
        } else {
            stop.complete();
        }
    }

    @Override
    public void start(Future<Void> start) {
        LOGGER.trace("About to start Verticle");
        LOGGER.info("Vertx uses LOGGER: {}, LoggerDelegate is {}", LOGGER, LOGGER.getDelegate());
        vertx.<RemoteCacheManager>executeBlocking(future -> {
            initConfiguration();
            RemoteCacheManager rcm = new RemoteCacheManager(getCacheManagerConfiguration());
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                LOGGER.debug("Exception caught while sleep(200) to wait for init {}", e, rcm);
            }
            LOGGER.info("Created RemoteCacheManager={}", rcm);
            future.complete(rcm);
        }, result -> {
            if (result.succeeded()) {
                manager = result.result();
                rootRouter = Router.router(vertx);
                registerManagementRestApi();
                allowCorsSupport(rootRouter);
                registerMBean();
                registerEventBusHadler();
                startHttpServer(start);
            } else {
                manager = null;
                LOGGER.fatal("Error while creating remote cache manager", result.cause());
                start.fail(result.cause());
            }
        });
    }

    protected void allowCorsSupport(Router router) {
        // CORS support
        Set<String> allowHeaders = new HashSet<String>();
        allowHeaders.add("x-requested-with");
        allowHeaders.add("Access-Control-Allow-Origin");
        allowHeaders.add("origin");
        allowHeaders.add("Content-Type");
        allowHeaders.add("accept");

        Set<HttpMethod> allowMethods = new HashSet<HttpMethod>();
        allowMethods.add(HttpMethod.GET);
        allowMethods.add(HttpMethod.POST);
        allowMethods.add(HttpMethod.DELETE);
        allowMethods.add(HttpMethod.PATCH);
        allowMethods.add(HttpMethod.PUT);

        router.route().handler(CorsHandler.create("*").allowedHeaders(allowHeaders).allowedMethods(allowMethods));
        router.route().handler(BodyHandler.create());
    }

    protected void registerManagementRestApi() {
        // https://www.devcon5.ch/en/blog/2017/09/15/vertx-modular-router-design/
        // throw new UnsupportedOperationException("Method is not implemented yet");
        Router sub = Router.router(vertx);
        sub.route("/info").handler(this::infoHandler);
        rootRouter.mountSubRouter("/" + getType(), sub);
        rootRouter.route("/").handler(this::restApiOnRoot);
    }

    protected void infoHandler(RoutingContext context) {
        context.response().putHeader("rc-type", "text/html").setStatusCode(HttpResponseStatus.OK.code())
                .end("<body><head><title>Info</title></head><body>Info</body></html>");
    }

    protected void restApiOnRoot(RoutingContext context) {
        HttpServerResponse response = context.response();
        response.putHeader("rc-type", "text/html").setStatusCode(HttpResponseStatus.OK.code());
        Buffer b = Buffer.buffer();
        b.appendString("<body><head><title>Endpoints</title></head><body><ul>\n");
        for (Route r : rootRouter.getRoutes()) {
            String path = r.getPath();
            if (path == null) {
                continue;
            }
            LOGGER.debug("Found path={} for route {}", path, r);
            b.appendString("<li><a href='").appendString(path).appendString("'>").appendString(path).appendString("</a></li>\n");
        }
        b.appendString("</ul></body></html>\n");
        response.putHeader("Content-Length", Integer.valueOf(b.length()).toString());
        response.write(b).end();
    }

    protected EventBus getEventBus() {
        if (eb == null) {
            eb = vertx.eventBus();
            LOGGER.info("Got reference for eventbus={}", eb);
        }
        return eb;
    }

    protected void unregisterEventBusHandler() {
        LOGGER.trace("About to Unregister EventBusHadler for address{}", eventBusAddress);
        getEventBus().consumer(eventBusAddress).unregister(result -> {
            if (result.succeeded()) {
                LOGGER.info("EventBusHadler unregistered for address={}", eventBusAddress);
            } else {
                LOGGER.error("Error while Unregistering EventBusHadler address={}", result.cause(), eventBusAddress);
            }
        });
    }

    protected void registerEventBusHadler() {
        LOGGER.trace("About to register EventBusHadler for address {}", eventBusAddress);
        getEventBus().consumer(eventBusAddress, this::defaultEventBusHandler).completionHandler(result -> {
            if (result.succeeded()) {
                LOGGER.info("EventBusHadler registered for address={}", eventBusAddress);
            } else {
                LOGGER.error("Error while registering EventBusHadler address={}", result.cause(), eventBusAddress);
            }
        });
    }

    protected void startHttpServer(Future<Void> future) {
        LOGGER.trace("Creating HTTP server for host={}, port={}", httpServerHost, httpServerPort);
        vertx.createHttpServer().requestHandler(rootRouter::accept).listen(httpServerPort, httpServerHost, result -> {
            if (result.succeeded()) {
                LOGGER.info("Vert.x HTTP Server started: " + result.result());
                future.complete();
            } else {
                LOGGER.error("Error while starting Vert.x HTTP Server", result.cause());
                // future.fail(result.cause());
                future.complete();
            }
        });
    }

    protected RemoteCache<UUID, AbstractDataObject> getCache() {
        if (cache == null) {
            LOGGER.debug("Trying to get cache: {}", serviceContextName);
            RemoteCache<UUID, AbstractDataObject> rc = manager.getCache(serviceContextName);
            cache = rc;
            LOGGER.info("Got reference for RemoteCahe={}", rc.getName());
        }
        return cache;
    }

    protected void resetCache() {
        // if (t instaceof ...) {
        cache = null;
        // }
    }

    protected void initConfiguration() {
        JsonObject vertxConfig = config();
        LOGGER.debug("Vert.x config: {}", vertxConfig);
        hotrodServerHost = vertxConfig.getString(SERVICE_JDG_REMOTE_ADDRESS.key, SERVICE_JDG_REMOTE_ADDRESS.value);
        hotrodServerPort = Integer
                .valueOf(vertxConfig.getString(SERVICE_JDG_REMOTE_PORT.key, SERVICE_JDG_REMOTE_PORT.value));
        httpServerHost = vertxConfig.getString(SERVICE_HTTP_LISTEN_ADDRESS.key, SERVICE_HTTP_LISTEN_ADDRESS.value);
        httpServerPort = Integer
                .valueOf(vertxConfig.getString(SERVICE_HTTP_LISTEN_PORT.key, SERVICE_HTTP_LISTEN_PORT.value));
        serviceContextName = vertxConfig.getString(SERVICE_NAMESPACE.key, "");
        eventBusAddress = vertxConfig.getString(SERVICE_EVENTBUS_PREFIX.key, SERVICE_EVENTBUS_PREFIX.value);
        if (serviceContextName.equals("") == false) {
            eventBusAddress += "." + serviceContextName;
        }
        final String info = "--->\nVerticle '{}' initialized with attributes:\n"
                + "INFINISPAN_HOTROD_SERVER_HOST - {}\nINFINISPAN_HOTROD_SERVER_PORT - {}\n"
                + "VERTX_HTTP_SERVER_HOST - {}\nVERTX_HTTP_SERVER_PORT - {}\nPUBLIC_CONTEXT_NAME - {}\n";
        LOGGER.info(info, getClass().getName(), hotrodServerHost, hotrodServerPort, httpServerHost, httpServerPort,
                serviceContextName);
    }

    protected Configuration getCacheManagerConfiguration() {
        LOGGER.trace("Creating remote cache configuration for host={}, port={}", hotrodServerHost, hotrodServerPort);
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.addServer().host(hotrodServerHost).port(hotrodServerPort);
        return builder.build();
    }

    protected void defaultEventBusHandler(Message<JsonObject> message) {
        LOGGER.trace("Rise defaultEventBusHandler for message {}", message);
        MultiMap headers = message.headers();
        String operation = headers.get(SERVICE_OPERATION.key);
        if (operation == null) {
            replyError(message, "Operation must be set at message header " + SERVICE_OPERATION.key);
            return;
        }

        customEventBusHandler(message, operation);

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

    protected void defaultCreateDataObject(Message<JsonObject> message) {
        JsonObject o = message.body();
        LOGGER.debug("Requesting to PUT into Cache for o={}...", o);

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

        // LOGGER.debug("Putting DataObject info cache: {}", ado.toJson());
        c.putAsync(id, ado).whenComplete((result, t) -> {
            LOGGER.debug("Cache PUT for id={}  completed with result: {}", id, t);
            if (t != null) {
                LOGGER.error("Error occured while working with cache", t);
                replyError(message, t.getCause().getMessage());
                return;
            }
            JsonObject reply = new JsonObject();
            reply.put("statusCode", HttpResponseStatus.CREATED.code());
            AbstractDataObject created = c.get(id);
            LOGGER.debug("COT just created DataObject: {}", created);
            reply.put("result", created.toJson());
            LOGGER.debug("Reply to publisher with {}", reply);
            message.reply(reply);
        });
    }

    protected void defaultGetDataObject(Message<JsonObject> message) {
        final UUID id = UUID.fromString(message.body().getString(HTTP_GET_PARAMETER_ID));
        LOGGER.debug("Requesting to GET Cache for id={}...", id);
        getCache().getAsync(id).whenComplete((result, t) -> {
            if (t != null) {
                LOGGER.error("Error occured while working with cache", t);
                replyError(message, t.getCause().getMessage());
                return;
            }
            JsonObject reply = new JsonObject();
            if (result == null) {
                LOGGER.debug("Object Not found for id={}", id);
                reply.put("statusCode", HttpResponseStatus.NOT_FOUND.code());
            } else {
                JsonObject json = result.toJson();
                LOGGER.debug("GOT Object {} from Cache", json);
                reply.put("statusCode", HttpResponseStatus.OK.code());
                reply.put("result", json);
                LOGGER.debug("Reply to publisher with {}", reply);
            }
            message.reply(reply);
        });
    }

    protected void defaultUpdateDataObject(Message<JsonObject> message) {
        JsonObject json = message.body();
        final UUID id = UUID.fromString(json.getString(HTTP_GET_PARAMETER_ID));
        LOGGER.debug("Requesting to UPDATE Cache for id={} for object {}...", id, json);

        AbstractDataObject ado = dataObjectFromJson(json);
        if (ado instanceof Versionable) {
            Versionable _new = (Versionable) ado;
            getCache().getAsync(id).whenComplete((result, t) -> {
                if (result instanceof Versionable) {
                    Versionable _old = (Versionable) result;
                    if (_old.isVersionEqual(_new)) {
                        // real update
                        realDefaultUpdateDataObject(message, ado, true);
                    } else {
                        replyError(message,
                                "Update object " + result + " with wrong versin " + ado.toJson().getString("version"));
                        // return;
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
            json.put("version", ((Versionable) _new).versionAsString());
            ado = dataObjectFromJson(json);
        } else {
            ado = _new;
        }

        getCache().replaceAsync(id, ado).whenComplete((result, t) -> {
            LOGGER.info("Cache REPLACE for id={} completed with result: {}", id, result);
            if (t != null) {
                LOGGER.error("Error occured while working with cache", t);
                replyError(message, t.getCause().getMessage());
                return;
            }
            JsonObject reply = new JsonObject();
            reply.put("statusCode", HttpResponseStatus.OK.code());
            reply.put("result", getCache().get(id).toJson());
            // reply.put("result", result.toJson());
            LOGGER.debug("Reply to publisher with {}", reply);
            message.reply(reply);
        });
    }

    protected void defaultRemoveDataObject(Message<JsonObject> message) {
        final UUID id = UUID.fromString(message.body().getString(HTTP_GET_PARAMETER_ID));
        LOGGER.debug("Requesting (HTTP/DELEETE) Cache for id={} for object {}...", id);
        getCache().removeAsync(id).whenCompleteAsync((result, t) -> {
            LOGGER.info("Cache DELETE for id={} completed with result: {}", id, result);
            if (t != null) {
                LOGGER.error("Error occured while working with cache", t);
                replyError(message, t.getCause().getMessage());
                return;
            }
            JsonObject reply = new JsonObject();
            reply.put("statusCode", HttpResponseStatus.NO_CONTENT.code());
            LOGGER.debug("Reply to publisher with {}", reply);
            message.reply(reply);
        });
    }

    private void replyError(Message<JsonObject> message, String msg) {
        // JsonObject reply = new JsonObject();
        resetCache();
        // reply.put("statusCode", );
        // reply.put("errorMessage", t.getMessage());
        // message.reply(reply);
        message.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), msg);
    }

    protected abstract void customEventBusHandler(Message<JsonObject> message, String operation);

    protected abstract AbstractDataObject dataObjectFromJson(JsonObject json);

    // protected abstract void registerRestApi();

    // Management methods
    @Override
    public String getServiceName() {
        return serviceContextName + "/" + getClass().getName();
    }

    // must be replaced with maven archetype generator with $package macro
    // return "${package}";
    protected abstract String getPackageName();
    // throw new UnsupportedOperationException("Method mustbe overriden in
    // subclass");
    // }

    // must be replaced with maven archetype generator with $package macro
    // return "type=${artefectId}";
    protected abstract String getType();
    // throw new UnsupportedOperationException("Method mustbe overrriden in
    // subclass");
    // }

    protected void registerMBean() {
        vertx.executeBlocking(future -> {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name;
            try {
                name = new ObjectName(getPackageName() + ":type=" + getType());
                mbs.registerMBean(this, name);
                future.complete();
            } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException
                    | NotCompliantMBeanException e) {
                future.fail(e);
            }
        }, result -> {
            if (result.succeeded()) {
                LOGGER.info("Registered JMX MBean '{}:Type={}'", getPackageName(), getType());
            } else {
                LOGGER.error("Error while creating JMX MBean '{}:Type={}'", result.cause(), getPackageName(),
                        getType());
            }
        });
    }
}