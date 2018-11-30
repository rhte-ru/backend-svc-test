package com.redhat.dsevosty.backend.account.svc;

import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_EVENTBUS_PREFIX;
import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_HTTP_LISTEN_ADDRESS;
import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_HTTP_LISTEN_PORT;
import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_JDG_REMOTE_ADDRESS;
import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_JDG_REMOTE_PORT;
import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_NAMESPACE;
import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_OPERATION;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.net.ServerSocket;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.redhat.dsevosty.backend.account.model.AccountDataObject;
import com.redhat.dsevosty.backend.util.InfinispanLocalHotrodServer;
import com.redhat.dsevosty.common.AccountStatusCode;
import com.redhat.dsevosty.common.model.AbstractDataObject;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
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
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
public class AccountDataGridVerticleTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccountDataGridVerticleTest.class);
    private static final String PUBLIC_CONTEXT_NAME = "account";
    private static final int DEFAULT_DELAY = 2;

    private static InfinispanLocalHotrodServer<UUID, AbstractDataObject> server;

    private EventBus sender;
    private String address;

    private UUID id;
    private AccountDataObject createdForUpdate = null;

    private static final AccountDataObject ADO = new AccountDataObject();
    private static int httpPort = 0;

    @BeforeAll
    public static void setUp(Vertx vertx, VertxTestContext context) throws InterruptedException {
        HotRodServerConfigurationBuilder serverConfig = new HotRodServerConfigurationBuilder()
                .host(SERVICE_JDG_REMOTE_ADDRESS.value).defaultCacheName(PUBLIC_CONTEXT_NAME)
                .port(Integer.valueOf(SERVICE_JDG_REMOTE_PORT.value));
        ConfigurationBuilder cacheConfig = new ConfigurationBuilder();
        server = new InfinispanLocalHotrodServer<UUID, AbstractDataObject>(cacheConfig.build(), serverConfig.build());
        server.getCache().put(ADO.getId(), ADO);

        DeploymentOptions options = new DeploymentOptions();
        JsonObject vertxConfig = new JsonObject();
        vertxConfig.put(SERVICE_JDG_REMOTE_ADDRESS.key, SERVICE_JDG_REMOTE_ADDRESS.value);
        vertxConfig.put(SERVICE_JDG_REMOTE_PORT.key, SERVICE_JDG_REMOTE_PORT.value);
        vertxConfig.put(SERVICE_NAMESPACE.key, PUBLIC_CONTEXT_NAME);
        try {
            ServerSocket socket = new ServerSocket(0);
            httpPort = socket.getLocalPort();
            socket.close();
        } catch (Exception e) {
            httpPort = Integer.valueOf(SERVICE_HTTP_LISTEN_PORT.value);
        }
        vertxConfig.put(SERVICE_HTTP_LISTEN_PORT.key, String.valueOf(httpPort));

        options.setConfig(vertxConfig);
        vertx.deployVerticle(AccountDataGridVerticle.class, options,
                context.succeeding(result -> context.completeNow()));
    }

    @AfterAll
    public static void teardDown(Vertx vertx, VertxTestContext context) throws InterruptedException {
        vertx.close(context.succeeding(ar -> {
            if (server != null) {
                server.stop();
            }
            context.completeNow();
        }));
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    @BeforeEach
    public void before(Vertx vertx, VertxTestContext context) {
        address = SERVICE_EVENTBUS_PREFIX.value + "." + PUBLIC_CONTEXT_NAME;
        sender = vertx.eventBus();
        LOGGER.trace("BeforeEach created client {}", sender);
        context.completeNow();
    }

    @AfterEach
    public void after(Vertx vertx, VertxTestContext context) throws InterruptedException {
        LOGGER.trace("AfterEach closing client {}", sender);
        // sender.close(result -> {
        // context.completeNow();
        // });
        context.completeNow();
        // context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    protected void handleCreateRightAnswer(AccountDataObject ado) {
        LOGGER.info("Account {} added", ado);
    }

    protected void handleCreateWrongAnswer(Throwable t) {
        LOGGER.trace("Got expected Exception", t);
    }

    protected void checkRightResult(AsyncResult<Message<JsonObject>> result, HttpResponseStatus status,
            VertxTestContext context) {
        checkRightResult(result, status, context, null);
    }

    protected void checkRightResult(AsyncResult<Message<JsonObject>> result, HttpResponseStatus status,
            VertxTestContext context, Handler<AccountDataObject> handler) {
        checkRightResult(result, status, context, null, handler);
    }

    protected void checkRightResult(AsyncResult<Message<JsonObject>> result, HttpResponseStatus status,
            VertxTestContext context, Checkpoint check, Handler<AccountDataObject> handler) {
        if (result.succeeded()) {
            try {
                JsonObject json = result.result().body();
                assertThat(json.getInteger("statusCode")).isEqualTo(status.code());
                JsonObject accountJson = json.getJsonObject("result");
                if (accountJson != null) {
                    AccountDataObject ado = new AccountDataObject(accountJson);
                    if (handler != null) {
                        handler.handle(ado);
                    }
                }
                if (check == null) {
                    context.completeNow();
                } else {
                    check.flag();
                }
            } catch (Throwable e) {
                context.failNow(e);
            }
        } else {
            final Throwable t = result.cause();
            LOGGER.error("Error occured while creating Account", t);
            context.failNow(t);
        }

    }

    protected void checkWrongResult(AsyncResult<Message<JsonObject>> result, HttpResponseStatus right,
            HttpResponseStatus wrong, VertxTestContext context, Handler<AccountDataObject> rightAnswer,
            Handler<Throwable> wrongAnswer) {
        checkWrongResult(result, right, wrong, context, null, rightAnswer, wrongAnswer);
    }

    protected void checkWrongResult(AsyncResult<Message<JsonObject>> result, HttpResponseStatus right,
            HttpResponseStatus wrong, VertxTestContext context, Checkpoint check,
            Handler<AccountDataObject> rightAnswer, Handler<Throwable> wrongAnswer) {
        if (result.succeeded()) {
            JsonObject json = result.result().body();
            assertThat(json.getInteger("statusCode")).isEqualTo(right.code());
            JsonObject accountJson = json.getJsonObject("result");
            if (accountJson != null) {
                AccountDataObject ado = new AccountDataObject(accountJson);
                if (rightAnswer != null) {
                    rightAnswer.handle(ado);
                }
            }
            context.failNow(new IllegalStateException("Account must not be created!!!"));
        } else {
            final Throwable t = result.cause();
            if (t instanceof ReplyException) {
                ReplyException e = (ReplyException) t;
                LOGGER.info("Expected error occured: {}, error code: {}", e.getMessage(), e.failureCode());
                assertThat(e.failureCode()).isEqualTo(wrong.code());
                if (wrongAnswer != null) {
                    wrongAnswer.handle(t);
                }
                if (check == null) {
                    context.completeNow();
                } else {
                    check.flag();
                }
            } else {
                LOGGER.error("Error occured while creating Account", t);
                context.failNow(t);
            }
        }
    }

    @Test
    public void createAccount(Vertx vertx, VertxTestContext context) throws InterruptedException {
        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "create");
        AccountDataObject _new = new AccountDataObject();
        sender.<JsonObject>send(address, _new.toJson(), options, result -> {
            checkRightResult(result, HttpResponseStatus.CREATED, context, this::handleCreateRightAnswer);
        });
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    @Test
    public void createAccountWithVersionMustBeFail(Vertx vertx, VertxTestContext context) throws InterruptedException {
        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "create");
        AccountDataObject _new = new AccountDataObject();
        _new.setVersion();
        sender.<JsonObject>send(address, _new.toJson(), options, result -> {
            checkWrongResult(result, HttpResponseStatus.CREATED, HttpResponseStatus.INTERNAL_SERVER_ERROR, context,
                    this::handleCreateRightAnswer, this::handleCreateWrongAnswer);
        });
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    @Test
    public void reCreateAlreadyExistedAccountMustBeFail(Vertx vertx, VertxTestContext context)
            throws InterruptedException {
        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "create");
        AccountDataObject _new = ADO;
        sender.<JsonObject>send(address, _new.toJson(), options, result -> {
            checkWrongResult(result, HttpResponseStatus.CREATED, HttpResponseStatus.INTERNAL_SERVER_ERROR, context,
                    this::handleCreateRightAnswer, this::handleCreateWrongAnswer);
        });
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    @Test
    public void getAccount(Vertx vertx, VertxTestContext context) throws InterruptedException {
        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "get");
        id = ADO.getId();
        sender.<JsonObject>send(address, new JsonObject().put("id", id.toString()), options, result -> {
            checkRightResult(result, HttpResponseStatus.OK, context, fetched -> {
                LOGGER.info("GOT reply form server: {}", fetched);
                assertThat(ADO).isEqualTo(fetched);
            });
        });
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    @Test
    public void getNotExistedAccount(Vertx vertx, VertxTestContext context) throws InterruptedException {
        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "get");
        id = ADO.defaultId();
        sender.<JsonObject>send(address, new JsonObject().put("id", id.toString()), options, result -> {
            checkRightResult(result, HttpResponseStatus.NOT_FOUND, context);
        });
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    @Test
    public void updateAccount(Vertx vertx, VertxTestContext context) throws InterruptedException {
        Checkpoint create = context.checkpoint();
        Checkpoint update = context.checkpoint();
        createdForUpdate = null;

        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "create");
        AccountDataObject _new = new AccountDataObject();
        sender.<JsonObject>send(address, _new.toJson(), options, result -> {
            checkRightResult(result, HttpResponseStatus.CREATED, context, create, fetched -> {
                LOGGER.info("GOT reply form server: {}", fetched.toJson());
                createdForUpdate = fetched;
                assertThat(_new).isEqualTo(createdForUpdate);
            });
        });

        context.awaitCompletion(1, TimeUnit.SECONDS);

        if (createdForUpdate == null) {
            context.failNow(new NullPointerException("Account has not created yet..."));
        }
        // createdForUpdate.setAmount(new BigDecimal("10000.00"));
        createdForUpdate.setStatus(AccountStatusCode.ACTIVE.name());
        options.addHeader(SERVICE_OPERATION.key, "update");
        sender.<JsonObject>send(address, createdForUpdate.toJson(), options, result -> {
            checkRightResult(result, HttpResponseStatus.OK, context, update, fetched -> {
                assertThat(createdForUpdate).isEqualTo(fetched);
                LOGGER.info("Updated account is " + fetched);
            });
        });
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    @Test
    public void updateAccountWithWrongVersionMustBeFail(Vertx vertx, VertxTestContext context)
            throws InterruptedException {
        Checkpoint create = context.checkpoint();
        Checkpoint update = context.checkpoint();
        createdForUpdate = null;

        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "create");
        AccountDataObject _new = new AccountDataObject();
        sender.<JsonObject>send(address, _new.toJson(), options, result -> {
            checkRightResult(result, HttpResponseStatus.CREATED, context, create, fetched -> {
                LOGGER.info("GOT reply form server: {}", fetched.toJson());
                createdForUpdate = fetched;
                assertThat(_new).isEqualTo(createdForUpdate);
            });
        });

        context.awaitCompletion(1, TimeUnit.SECONDS);

        if (createdForUpdate == null) {
            context.failNow(new NullPointerException("Account has not created yet..."));
        }
        createdForUpdate.setAmount(new BigDecimal("10000.00"));
        options.addHeader(SERVICE_OPERATION.key, "update");
        sender.<JsonObject>send(address, createdForUpdate.toJson().put("version", UUID.randomUUID().toString()),
                options, result -> {
                    checkWrongResult(result, HttpResponseStatus.OK, HttpResponseStatus.INTERNAL_SERVER_ERROR, context,
                            update, rightAnswer -> {
                                assertThat(createdForUpdate).isEqualTo(rightAnswer);
                                LOGGER.info("Updated account is " + rightAnswer);
                            }, this::handleCreateWrongAnswer);
                });
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    protected void httpResponseHandler(AsyncResult<HttpResponse<Buffer>> result, VertxTestContext context, Handler<Buffer> bodyHandler) {
        if (result.succeeded()) {
            try {
                final HttpResponse<Buffer> response = result.result();
                assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                final Buffer body = response.body();
                LOGGER.debug("GOT body:\n{}", body);
                if (bodyHandler != null) {
                    bodyHandler.handle(body);
                    context.completeNow();
                }
            } catch (Throwable t) {
                context.failNow(t);
            }
        } else {
            LOGGER.error("Error occured while asking http://{}:{}", result.cause(), SERVICE_HTTP_LISTEN_ADDRESS.value, httpPort);
            context.failNow(result.cause());
        }
    }

    // @Test
    public void testRestApiOnRoot(Vertx vertx, VertxTestContext context) throws InterruptedException {
        WebClient web = WebClient.create(vertx);
        web.get(httpPort, SERVICE_HTTP_LISTEN_ADDRESS.value, "/").send(response -> {
            httpResponseHandler(response, context, body -> {
                assertThat(body.toString()).contains("<title>Endpoints</title>");
            });
        });
        web.close();
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }

    // @Test
    public void testInfoHandler(Vertx vertx, VertxTestContext context) throws InterruptedException {
        WebClient web = WebClient.create(vertx);
        web.get(httpPort, SERVICE_HTTP_LISTEN_ADDRESS.value, "/account/info").send(response -> {
            httpResponseHandler(response, context, body -> {
                assertThat(body.toString()).contains("<title>Info</title>");
            });
        });
        web.close();
        context.awaitCompletion(DEFAULT_DELAY, TimeUnit.SECONDS);
    }
}
