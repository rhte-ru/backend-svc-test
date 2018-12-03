package com.redhat.dsevosty.common.svc.adapter;

import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_OPERATION;

import com.redhat.dsevosty.common.svc.CommonVerticle;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class CommonHttpAdapter extends CommonVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonHttpAdapter.class);

    public static final String ARTIFACT_ID = "http_adapter";

    private Router apiRouter;

    @Override
    public void start(Future<Void> start) {
        super.start(start);
        registerDefaultRestApi();
    }

    protected synchronized Router getApiRouter() {
        if (apiRouter == null) {
            apiRouter = Router.router(vertx);
        }
        return apiRouter;
    }

    @Override
    protected Router createRouterHierarchy() {
        Router root = super.createRouterHierarchy();
        Router sub = getSubRouter();
        Router api = getApiRouter();
        sub.mountSubRouter("/", api);
        return root;
    }

    protected void registerDefaultRestApi() {
        Router router = getApiRouter();

        router.get("/:id").handler(this::getDataObject);
        router.post("/").handler(this::addDataObject);
        // router.put("/:id").handler(this::updateSDO);
        // router.patch("/:id").handler(this::updateSDO);
        // router.delete("/:id").handler(this::removeSDO);
    }

    private void sendError(RoutingContext rc, String id, Throwable th) {
        final String msg = "Error occured while asking service " + getServiceName() + " for key " + id;
        LOGGER.error(msg, th);
        rc.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
        rc.fail(th);
    }

    protected void getDataObject(RoutingContext rc) {
        final String id = rc.request().getParam(HTTP_GET_PARAMETER_ID);
        LOGGER.trace("Handling GET request for id: {}", id);
        if (id == null || id.equals("")) {
            sendError(rc, id,
                    new IllegalArgumentException("There is no an " + HTTP_GET_PARAMETER_ID + " or it is NULL"));
            return;
        }
        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "get");
        getEventBus().<JsonObject>send(getEventBusAddress(), new JsonObject().put("id", id), options, result -> {
            if (result.succeeded()) {
                JsonObject json = result.result().body();
                final int status = json.getInteger("statusCode");
                final HttpServerResponse response = rc.response();
                if (status == HttpResponseStatus.OK.code()) {
                    response.setStatusCode(HttpResponseStatus.OK.code()).putHeader("context-type", "application/json");
                    if (LOGGER.isDebugEnabled()) {
                        response.end(json.encodePrettily());
                    } else {
                        response.end(json.encode());
                    }
                    return;
                }
                if (status == HttpResponseStatus.NOT_FOUND.code()) {
                    response.setStatusCode(HttpResponseStatus.NOT_FOUND.code()).end();
                    return;
                }
                sendError(rc, id, new IllegalArgumentException(
                        "Unexpected answer code " + status + " form service " + getServiceName() + " for key" + id));
            } else {
                sendError(rc, id, result.cause());
            }
        });
    }

    protected void addDataObject(RoutingContext rc) {
        final JsonObject json = rc.getBodyAsJson();
        LOGGER.trace("Handlilng POST request for: ", json);
        final String id = json.getString(HTTP_GET_PARAMETER_ID);
        if (id == null || id.equals("")) {
            sendError(rc, id,
                    new IllegalArgumentException("There is no an " + HTTP_GET_PARAMETER_ID + " or it is NULL"));
            return;
        }
        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "create");
        getEventBus().<JsonObject>send(getEventBusAddress(), json, options, result -> {
            if (result.succeeded()) {
                JsonObject answer = result.result().body();
                final int status = json.getInteger("statusCode");
                final HttpServerResponse response = rc.response();
                if (status == HttpResponseStatus.CREATED.code()) {
                    response.setStatusCode(HttpResponseStatus.OK.code()).putHeader("context-type", "application/json");
                    if (LOGGER.isDebugEnabled()) {
                        response.end(answer.encodePrettily());
                    } else {
                        response.end(answer.encode());
                    }
                    return;
                }
                sendError(rc, id, new IllegalArgumentException(
                        "Unexpected answer code " + status + " form service " + getServiceName() + " for key" + id));
            } else {
                sendError(rc, id, result.cause());
            }
        });
    }

    @Override
    protected void defaultEventBusHandler(Message<JsonObject> message) {

    }

    @Override
    protected String getType() {
        return ARTIFACT_ID;
    }
}
