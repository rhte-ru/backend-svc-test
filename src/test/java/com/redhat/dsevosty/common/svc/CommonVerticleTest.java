package com.redhat.dsevosty.common.svc;

import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_HTTP_LISTEN_ADDRESS;
import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_HTTP_LISTEN_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
public class CommonVerticleTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonVerticleTest.class);
    private static final int DEFAULT_DELAY = 2;
    private static int httpPort = 0;

    private WebClient web;

    @BeforeAll
    public static void setUp(Vertx vertx, VertxTestContext context) throws InterruptedException {
        DeploymentOptions options = new DeploymentOptions();
        JsonObject vertxConfig = new JsonObject();
        try {
            ServerSocket socket = new ServerSocket(0);
            httpPort = socket.getLocalPort();
            socket.close();
        } catch (Exception e) {
            httpPort = Integer.valueOf(SERVICE_HTTP_LISTEN_PORT.value);
        }
        vertxConfig.put(SERVICE_HTTP_LISTEN_PORT.key, String.valueOf(httpPort));

        options.setConfig(vertxConfig);
        vertx.deployVerticle(MyCommonVerticle.class, options, ar -> {
            if (ar.succeeded()) {
                context.completeNow();
            } else {
                LOGGER.fatal(ar.cause());
                context.failNow(ar.cause());
            }
        });
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    @AfterAll
    public static void teardDown(Vertx vertx, VertxTestContext context) throws InterruptedException {
        vertx.close(result -> context.completeNow());
    }

    @BeforeEach
    public void before(Vertx vertx, VertxTestContext context) {
        web = WebClient.create(vertx);
        context.completeNow();
    }

    @AfterEach
    public void after(Vertx vertx, VertxTestContext context) {
        if (web != null) {
            web.close();
        }
        context.completeNow();
    }

    protected void httpResponseHandler(AsyncResult<HttpResponse<Buffer>> result, VertxTestContext context,
            Handler<Buffer> bodyHandler) {
        if (result.succeeded()) {
            try {
                final HttpResponse<Buffer> response = result.result();
                assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                final Buffer body = response.body();
                LOGGER.debug("GOT body:\n{}", body);
                if (bodyHandler != null) {
                    bodyHandler.handle(body);
                }
                context.completeNow();
            } catch (Throwable t) {
                context.failNow(t);
            }
        } else {
            LOGGER.error("Error occured while asking http://{}:{}", result.cause(), SERVICE_HTTP_LISTEN_ADDRESS.value,
                    httpPort);
            context.failNow(result.cause());
        }
    }

    @Test
    public void testRestApiOnRoot(Vertx vertx, VertxTestContext context) throws InterruptedException {
        web.get(httpPort, SERVICE_HTTP_LISTEN_ADDRESS.value, "/").send(response -> {
            httpResponseHandler(response, context, body -> {
                assertThat(body.toString()).contains("\"paths\" : [ \"");
            });
        });
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    @Test
    public void testMgmtInfoHandler(Vertx vertx, VertxTestContext context) throws InterruptedException {
        web.get(httpPort, SERVICE_HTTP_LISTEN_ADDRESS.value, "/common/info").send(response -> {
            httpResponseHandler(response, context, body -> {
                assertThat(body.toString()).contains("\"methods\" : [ \"");
            });
        });
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    @Test
    public void testMgmtGetHttpHost(Vertx vertx, VertxTestContext context) throws InterruptedException {
        web.get(httpPort, SERVICE_HTTP_LISTEN_ADDRESS.value, "/common/management/httpserverhost").send(response -> {
            httpResponseHandler(response, context, body -> {
                assertThat(body.toString()).contains("\"result\" : \"");
            });
        });
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    @Test
    public void testMgmtSetHttpHost(Vertx vertx, VertxTestContext context) throws InterruptedException {
        final String address = "0.0.0.0";
        web.post(httpPort, SERVICE_HTTP_LISTEN_ADDRESS.value, "/common/management/httpserverhost")
                .sendJsonObject(new JsonObject().put("value", address), response -> {
                    httpResponseHandler(response, context, body -> {
                        assertThat(body.toString()).contains("\"result\" : \"" + address);
                    });
                });
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    @Test
    public void testMgmtGetHttpPort(Vertx vertx, VertxTestContext context) throws InterruptedException {
        web.get(httpPort, SERVICE_HTTP_LISTEN_ADDRESS.value, "/common/management/httpserverport").send(response -> {
            httpResponseHandler(response, context, body -> {
                assertThat(body.toString()).contains("\"result\" : ");
            });
        });
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    @Test
    public void testMgmtSetHttpPort(Vertx vertx, VertxTestContext context) throws InterruptedException {
        web.post(httpPort, SERVICE_HTTP_LISTEN_ADDRESS.value, "/common/management/httpserverport")
                .sendJsonObject(new JsonObject().put("value", Integer.valueOf(SERVICE_HTTP_LISTEN_PORT.value)), response -> {
                    httpResponseHandler(response, context, body -> {
                        assertThat(body.toString()).contains("\"result\" : " + SERVICE_HTTP_LISTEN_PORT.value);
                    });
                });
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    @Test
    public void testMgmtGetServiceName(Vertx vertx, VertxTestContext context) throws InterruptedException {
        web.get(httpPort, SERVICE_HTTP_LISTEN_ADDRESS.value, "/common/management/servicename").send(response -> {
            httpResponseHandler(response, context, body -> {
                assertThat(body.toString()).contains("\"result\" : \"");
            });
        });
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    public static class MyCommonVerticle extends CommonVerticle {
        public static final String PACKAGE_NAME = MyCommonVerticle.class.getPackage().getName();
        public static final String ARTIFACT_ID = "common";

        @Override
        protected void defaultEventBusHandler(Message<JsonObject> message) {
        }

        @Override
        protected String getPackageName() {
            return PACKAGE_NAME;
        }

        @Override
        protected String getType() {
            return ARTIFACT_ID;
        }
    }

}
