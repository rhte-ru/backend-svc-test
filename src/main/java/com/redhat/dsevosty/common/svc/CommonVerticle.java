package com.redhat.dsevosty.common.svc;

import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_EVENTBUS_PREFIX;
import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_HTTP_LISTEN_ADDRESS;
import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_HTTP_LISTEN_PORT;
import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_NAMESPACE;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

public abstract class CommonVerticle extends AbstractVerticle implements CommonVerticleMBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(CommonVerticle.class);

  protected static final String HTTP_GET_PARAMETER_ID = "id";

  protected String serviceContextName;

  private HttpServer httpServer;
  private String httpServerHost;
  private int httpServerPort;

  private EventBus eb;
  protected String eventBusAddress;

  protected Router rootRouter;

  private List<Method> managementMethods;

  @Override
  public void start(Future<Void> start) {
    LOGGER.info("About to start Verticle");
    LOGGER.info("Vertx uses LOGGER: {}, LoggerDelegate is {}", LOGGER, LOGGER.getDelegate());
    registerMBean();
    printInitialConfiguration(initConfiguration());
    rootRouter = Router.router(vertx);
    allowCorsSupport(rootRouter);
    registerManagementRestApi();
    registerEventBusHandler();
    startHttpServer(start);
  }

  @Override
  public void stop(Future<Void> stop) {
    LOGGER.info("About to stop Verticle({})", this);
    stopHttpServer(stop);
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

  protected Future<Void> startHttpServer(Future<Void> future) {
    LOGGER.info("Creating HTTP server for host={}, port={}", httpServerHost, httpServerPort);
    vertx.createHttpServer().requestHandler(rootRouter::accept).listen(httpServerPort, httpServerHost, result -> {
      httpServerStartErrorHandler(future, result);
    });
    return future;
  }

  protected void httpServerStartErrorHandler(Future<Void> future, AsyncResult<HttpServer> result) {
    if (result.succeeded()) {
      LOGGER.info("Vert.x HTTP Server started: " + result.result());
      future.complete();
    } else {
      LOGGER.fatal("Error while starting Vert.x HTTP Server", result.cause());
      future.fail(result.cause());
    }
  }

  protected void stopHttpServer(Future<Void> future) {
    LOGGER.info("About to stopping HTTP server for host={}, port={}", httpServerHost, httpServerPort);
    if (httpServer != null) {
      httpServer.close(ar -> {
        if (ar.failed()) {
          LOGGER.error("Error occured while stopping Vert.x HTTP server: ", ar.cause());
        }
        future.complete();
      });
      httpServer = null;
    } else {
      future.complete();
    }
  }

  protected EventBus getEventBus() {
    if (eb == null) {
      eb = vertx.eventBus();
      LOGGER.info("Got reference for eventbus={}", eb);
    }
    return eb;
  }

  protected String initConfiguration() {
    JsonObject vertxConfig = config();
    LOGGER.debug("Vert.x config: {}", vertxConfig);
    httpServerHost = vertxConfig.getString(SERVICE_HTTP_LISTEN_ADDRESS.key, SERVICE_HTTP_LISTEN_ADDRESS.value);
    httpServerPort = Integer
        .valueOf(vertxConfig.getString(SERVICE_HTTP_LISTEN_PORT.key, SERVICE_HTTP_LISTEN_PORT.value));
    serviceContextName = vertxConfig.getString(SERVICE_NAMESPACE.key, "");
    eventBusAddress = vertxConfig.getString(SERVICE_EVENTBUS_PREFIX.key, SERVICE_EVENTBUS_PREFIX.value);
    if (serviceContextName.equals("") == false) {
      eventBusAddress += "." + serviceContextName;
    }
    final String info = "--->\nVerticle '{}' initialized with attributes:\n"
        + "SERVICE_HTTP_LISTEN_ADDRESS - {}\nSERVICE_HTTP_LISTEN_PORT - {}\nSERVICE_NAMESPACE - {}\n"
        + "EVENT_BUS_ADDRESS - {}\n";
    return info;
  }

  protected void printInitialConfiguration(String info) {
    LOGGER.info(info, getClass().getName(), httpServerHost, httpServerPort, serviceContextName, eventBusAddress);
  }

  protected void registerMBean() {
    vertx.executeBlocking(future -> {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      try {
        final ObjectName name = new ObjectName(getPackageName() + ":type=" + getType());
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
        LOGGER.error("Error while creating JMX MBean '{}:Type={}'", result.cause(), getPackageName(), getType());
      }
    });
  }

  protected abstract void defaultEventBusHandler(Message<JsonObject> message);

  // Managenent Methods
  @Override
  public String getServiceName() {
    return serviceContextName + "/" + getClass().getName();
  }

  @Override
  public void createHttpServer() {
    Future<Void> future = Future.<Void>future();
    future.setHandler(ar -> {
      if (ar.succeeded()) {
        LOGGER.info("HTTP Server has started successfully");
      } else {
        LOGGER.info("Error occured while restarting HTTP Server: ", ar.cause());
      }
    });
    startHttpServer(future);
  }

  @Override
  public void destroyHttpServer() {
    Future<Void> future = Future.<Void>future();
    future.setHandler(ar -> {
      if (ar.succeeded()) {
        LOGGER.info("HTTP Server has stopped successfully");
      } else {
        LOGGER.info("Error occured while stopping HTTP Server: ", ar.cause());
      }
    });
    stopHttpServer(future);
  }

  @Override
  public void setHttpServerHost(String host) {
    this.httpServerHost = host;
  }

  @Override
  public String getHttpServerHost() {
    return httpServerHost;
  }

  @Override
  public void setHttpServerPort(int port) {
    this.httpServerPort = port;
  }

  @Override
  public int getHttpServerPort() {
    return httpServerPort;
  }

  @Override
  public void registerManagementRestApi() {
    // https://www.devcon5.ch/en/blog/2017/09/15/vertx-modular-router-design/
    // throw new UnsupportedOperationException("Method is not implemented yet");
    Router sub = Router.router(vertx);
    sub.route("/info").handler(this::infoHandler);
    Router mgmt = Router.router(vertx);
    rootRouter.route("/").handler(this::restApiOnRoot);
    rootRouter.mountSubRouter("/" + getType() + "/", sub);
    sub.mountSubRouter("/management/", mgmt);
    mgmtHandler(mgmt);
  }

  protected List<Class<?>> getImplementedInterfaces(Class<?> clazz) {
    List<Class<?>> list = new ArrayList<Class<?>>();
    Class<?>[] ifs = clazz.getInterfaces();
    // LOGGER.debug("interfaces length: {}", ifs.length);
    if (ifs != null) {
      list.addAll(Arrays.asList(ifs));
    }
    // LOGGER.debug("interfaces list length: {}", list.size());
    Class<?> zuper = clazz.getSuperclass();
    if (zuper == null) {
      // LOGGER.debug("zuper is null");
      return list;
    }
    List<Class<?>> nested = getImplementedInterfaces(zuper);
    // LOGGER.debug("nested list size: {}", nested.size());
    if (nested.size() > 0) {
      list.addAll(nested);
    }
    // LOGGER.debug("returned list size: {}", list.size());
    return list;
  }

  protected List<Method> getManagementMethods() {
    LOGGER.trace("About to get Management Methods");
    if (managementMethods != null) {
      LOGGER.trace("Already existed methods list is: {}", managementMethods);
      return managementMethods;
    }
    List<Class<?>> classes = getImplementedInterfaces(getClass());
    LOGGER.debug("Class {} implements {} interfaces", getClass().getName(), classes.size());
    List<Method> methods = new ArrayList<Method>();
    List<Class<?>> mbeans = new ArrayList<Class<?>>();
    for (Class<?> clazz : classes) {
      LOGGER.trace("Analizing interface: " + clazz);
      if (clazz.getName().endsWith("MBean")) {
        if (Modifier.isPublic(clazz.getModifiers())) {
          LOGGER.trace("Found MBean interface with name: {}", clazz.getSimpleName());
          mbeans.add(clazz);
        }
      }
    }
    for (Class<?> clazz : mbeans) {
      final Stream<Method> s = Arrays.asList(clazz.getDeclaredMethods()).stream();
      final List<Method> list = s.filter(m -> {
        return Modifier.isPublic(m.getModifiers());
        // final String name = m.getName();
        // return (name.startsWith("get") || name.startsWith("set") ||
        // name.startsWith("is"))
        // && Modifier.isPublic(m.getModifiers());
      }).collect(Collectors.toList());
      methods.addAll(list);
    }
    // methods.sort((s1, s2) ->
    // s1.getName().substring(1).compareTo(s2.getName().substring(1)));
    managementMethods = methods;
    return managementMethods;
  }

  protected void infoHandler(RoutingContext context) {
    LOGGER.info("Registering handler for: {}", context.normalisedPath());
    JsonObject json = new JsonObject();
    json.put("description", "Available management method(s)");
    JsonArray list = new JsonArray();
    json.put("methods", list);
    for (Method m : getManagementMethods()) {
      list.add(m.getName());
    }
    final String resp = json.encodePrettily();
    LOGGER.debug("Send jsonResponse to client: ", resp);
    context.response().putHeader("rc-type", "text/json").setStatusCode(HttpResponseStatus.OK.code()).end(resp);
  }

  protected void mgmtHandler(Router router) {
    LOGGER.info("Registering handler for: {}", router.route().getPath());
    for (Method m : getManagementMethods()) {
      final String name = m.getName().toLowerCase();
      boolean getter = true;
      String path = name;
      if (name.startsWith("get")) {
        path = name.substring(3);
      }
      if (name.startsWith("is")) {
        path = name.substring(3);
      }
      if (name.startsWith("set")) {
        path = name.substring(3);
        getter = false;
      }
      // if (name.startsWith("get")) {
      // path = name.substring(3);
      // } else {
      // if (name.startsWith("set")) {
      // path = name.substring(3);
      // getter = false;
      // } else {
      // if (name.startsWith("is")) {
      // path = name.substring(2);
      // } else {
      // throw new UnsupportedOperationException("Cann't set handler for method: " +
      // name);
      // }
      // }
      // }
      if (getter) {
        LOGGER.debug("Registering handler for context {}, method GET", path);
        router.get("/" + path).handler(rc -> {
          try {
            final Object result = m.invoke(this);
            JsonObject json = new JsonObject();
            json.put("method", name);
            json.put("result", result);
            final String resp = json.encodePrettily();
            LOGGER.trace("Send jsonResponse to client: {}", resp);
            rc.response().setStatusCode(HttpResponseStatus.OK.code()).end(resp);
            return;
          } catch (Exception e) {
            LOGGER.error("Error occured while invoke {} {}", e, Method.class.getName(), name);
            rc.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end(e.getMessage());
            return;
          }
        });
      } else {
        LOGGER.debug("Registering handler for context {}, method POST", path);
        router.post("/" + path).handler(rc -> {
          JsonObject body = rc.getBodyAsJson();
          final Object value = body.getValue("value");
          try {
            m.invoke(this, value);
            JsonObject json = new JsonObject();
            json.put("method", name);
            json.put("result", value);
            final String resp = json.encodePrettily();
            LOGGER.trace("Send jsonResponse to client: ", resp);
            rc.response().setStatusCode(HttpResponseStatus.OK.code()).end(resp);
            return;
          } catch (Exception e) {
            LOGGER.error("Error occured while invoke {} {}", e, Method.class.getName(), name);
            rc.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end(e.getMessage());
            return;
          }
        });
      }
    }
  }

  protected void restApiOnRoot(RoutingContext rc) {
    LOGGER.info("Registering handler for: {}", rc.normalisedPath());
    JsonObject resp = new JsonObject();
    JsonArray path = new JsonArray();
    resp.put("description", "Known API endpoints");
    resp.put("paths", path);
    for (Route r : rootRouter.getRoutes()) {
      // rc.
      String p = r.getPath();
      if (p == null) {
        continue;
      }
      LOGGER.debug("Found path={} for route {}", p, r);
      path.add(p);
    }
    final String response = resp.encodePrettily();
    rc.response().putHeader("rc-type", "text/json").setStatusCode(HttpResponseStatus.OK.code()).end(response);
  }

  @Override
  public void unregisterManagementRestApi() {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public void registerEventBusHandler() {
    LOGGER.info("About to register EventBusHadler for address {}", eventBusAddress);
    getEventBus().consumer(eventBusAddress, this::defaultEventBusHandler).completionHandler(result -> {
      if (result.succeeded()) {
        LOGGER.info("EventBusHadler registered for address={}", eventBusAddress);
      } else {
        LOGGER.error("Error while registering EventBusHadler address={}", result.cause(), eventBusAddress);
      }
    });
  }

  @Override
  public void unregisterEventBusHandler() {
    LOGGER.info("About to Unregister EventBusHadler for address{}", eventBusAddress);
    getEventBus().consumer(eventBusAddress).unregister(result -> {
      if (result.succeeded()) {
        LOGGER.info("EventBusHadler unregistered for address={}", eventBusAddress);
      } else {
        LOGGER.error("Error while Unregistering EventBusHadler address={}", result.cause(), eventBusAddress);
      }
    });
  }

  // must be replaced with maven archetype generator with $package macro
  // return "${package}";
  protected abstract String getPackageName();

  // must be replaced with maven archetype generator with $package macro
  // return "type=${artefectId}";
  protected abstract String getType();

}
