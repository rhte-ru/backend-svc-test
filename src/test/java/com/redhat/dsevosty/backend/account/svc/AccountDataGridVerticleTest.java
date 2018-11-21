package com.redhat.dsevosty.backend.account.svc;

import static com.redhat.dsevosty.common.ServiceConstant.SERVICE_EVENTBUS_PREFIX;
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

import javax.activation.UnsupportedDataTypeException;

import com.redhat.dsevosty.backend.account.model.AccountDataObject;
import com.redhat.dsevosty.backend.util.InfinispanLocalHotrodServer;
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
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
public class AccountDataGridVerticleTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccountDataGridVerticleTest.class);
    private static final String PUBLIC_CONTEXT_NAME = "account";

    // private static InfinispanLocalHotrodServer<UUID, AccountDataObject> server;
    private static InfinispanLocalHotrodServer<UUID, AbstractDataObject> server;

    private EventBus sender;
    private String address;

    private UUID id;
    private AccountDataObject createdForUpdate = null;

    private static final AccountDataObject ADO = new AccountDataObject();

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
        int httpPort;
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
        context.awaitCompletion(15, TimeUnit.SECONDS);
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

    // @Test
    public void createAccount(Vertx vertx, VertxTestContext context) throws InterruptedException {
        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "create");
        AccountDataObject _new = new AccountDataObject();
        sender.send(address, _new.toJson(), options, result -> {
            if (result.succeeded()) {
                Object answer = result.result().body();
                if (answer instanceof JsonObject) {
                    JsonObject json = (JsonObject) answer;
                    assertThat(json.getInteger("statusCode")).isEqualTo(HttpResponseStatus.CREATED.code());
                    AccountDataObject created = new AccountDataObject(json.getJsonObject("result"));
                    LOGGER.info("Account {} added", created);
                    context.completeNow();
                } else {
                    final String msg = "Usupported object class " + answer.getClass().getName();
                    LOGGER.error(msg);
                    context.failNow(new UnsupportedDataTypeException(msg));
                }
            } else {
                final Throwable t = result.cause();
                LOGGER.error("Error occured while creating Account", t);
                context.failNow(t);
            }
        });
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    // @Test
    public void createAccountWithVersionMustBefail(Vertx vertx, VertxTestContext context) throws InterruptedException {
        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "create");
        AccountDataObject _new = new AccountDataObject();
        // AccountDataObject _new = ADO;
        _new.setVersion();
        sender.send(address, _new.toJson(), options, result -> {
            if (result.succeeded()) {
                Object answer = result.result().body();
                if (answer instanceof JsonObject) {
                    JsonObject json = (JsonObject) answer;
                    assertThat(json.getInteger("statusCode")).isEqualTo(HttpResponseStatus.CREATED.code());
                    AccountDataObject created = new AccountDataObject(json.getJsonObject("result"));
                    LOGGER.info("Account {} added", created);
                    context.failNow(new IllegalStateException("Account must not be created!!!"));
                } else {
                    final String msg = "Usupported object class " + answer.getClass().getName();
                    LOGGER.error(msg);
                    context.failNow(new UnsupportedDataTypeException(msg));
                }
            } else {
                final Throwable t = result.cause();
                if (t instanceof ReplyException) {
                    ReplyException e = (ReplyException) t;
                    LOGGER.debug("Expected error occured: {} with code: {}", e.getMessage(), e.failureCode());
                    assertThat(e.failureCode()).isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                    context.completeNow();
                } else {
                    LOGGER.error("Error occured while creating Account", t);
                    context.failNow(t);
                }
            }
        });
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    // @Test
    public void reCreateAlreadyExistedAccountMustBefail(Vertx vertx, VertxTestContext context)
            throws InterruptedException {
        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "create");
        AccountDataObject _new = ADO;
        sender.send(address, _new.toJson(), options, result -> {
            if (result.succeeded()) {
                Object answer = result.result().body();
                if (answer instanceof JsonObject) {
                    JsonObject json = (JsonObject) answer;
                    assertThat(json.getInteger("statusCode")).isEqualTo(HttpResponseStatus.CREATED.code());
                    AccountDataObject created = new AccountDataObject(json.getJsonObject("result"));
                    LOGGER.info("Account {} added", created);
                    context.failNow(new IllegalStateException("Account must not be created!!!"));
                } else {
                    final String msg = "Usupported object class " + answer.getClass().getName();
                    LOGGER.error(msg);
                    context.failNow(new UnsupportedDataTypeException(msg));
                }
            } else {
                final Throwable t = result.cause();
                if (t instanceof ReplyException) {
                    ReplyException e = (ReplyException) t;
                    LOGGER.debug("Expected error occured: {} with code: {}", e.getMessage(), e.failureCode());
                    assertThat(e.failureCode()).isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                    context.completeNow();
                } else {
                    LOGGER.error("Error occured while creating Account", t);
                    context.failNow(t);
                }
            }
        });
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    // @Test
    public void getAccount(Vertx vertx, VertxTestContext context) throws InterruptedException {
        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "get");
        id = ADO.defaultId();
        sender.send(address, new JsonObject().put("id", id.toString()), options, result -> {
            if (result.succeeded()) {
                Object answer = result.result().body();
                if (answer instanceof JsonObject) {
                    JsonObject json = (JsonObject) answer;
                    try {
                        final int statusCode = json.getInteger("statusCode");
                        assertThat(statusCode).isEqualTo(HttpResponseStatus.OK.code());
                        AccountDataObject fetched = new AccountDataObject(json.getJsonObject("result"));
                        LOGGER.info("GOT reply form server: {}", fetched);
                        assertThat(ADO).isEqualTo(fetched);
                        context.completeNow();
                    } catch (Throwable e) {
                        context.failNow(e);
                    }
                } else {
                    final String msg = "Usupported object class " + answer.getClass().getName();
                    LOGGER.error(msg);
                    context.failNow(new UnsupportedDataTypeException(msg));
                }
            } else {
                final Throwable t = result.cause();
                LOGGER.error("Error occured while fetching Account for id=" + id, t);
                context.failNow(t);
            }
        });
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    // @Test
    public void getNotExistedAccount(Vertx vertx, VertxTestContext context) throws InterruptedException {
        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "get");
        id = ADO.defaultId();
        sender.send(address, new JsonObject().put("id", id.toString()), options, result -> {
            if (result.succeeded()) {
                Object answer = result.result().body();
                if (answer instanceof JsonObject) {
                    JsonObject json = (JsonObject) answer;
                    try {
                        final int statusCode = json.getInteger("statusCode");
                        assertThat(statusCode).isEqualTo(HttpResponseStatus.NOT_FOUND.code());
                        context.completeNow();
                    } catch (Throwable e) {
                        context.failNow(e);
                    }
                } else {
                    final String msg = "Usupported object class " + answer.getClass().getName();
                    LOGGER.error(msg);
                    context.failNow(new UnsupportedDataTypeException(msg));
                }
            } else {
                final Throwable t = result.cause();
                LOGGER.error("Error occured while fetching Account for id=" + id, t);
                context.failNow(t);
            }
        });
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    // @Test
    public void updateAccount(Vertx vertx, VertxTestContext context) throws InterruptedException {
        Checkpoint create = context.checkpoint();
        Checkpoint update = context.checkpoint();
        createdForUpdate = null;

        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "create");
        AccountDataObject _new = new AccountDataObject();
        sender.send(address, _new.toJson(), options, result -> {
            if (result.succeeded()) {
                Object answer = result.result().body();
                if (answer instanceof JsonObject) {
                    JsonObject json = (JsonObject) answer;
                    try {
                        final int statusCode = json.getInteger("statusCode");
                        assertThat(statusCode).isEqualTo(HttpResponseStatus.CREATED.code());
                        createdForUpdate = new AccountDataObject(json.getJsonObject("result"));
                        assertThat(_new).isEqualTo(createdForUpdate);
                        create.flag();
                    } catch (Throwable e) {
                        context.failNow(e);
                    }
                } else {
                    final String msg = "Usupported object class " + answer.getClass().getName();
                    LOGGER.error(msg);
                    context.failNow(new UnsupportedDataTypeException(msg));
                }
            } else {
                final Throwable t = result.cause();
                LOGGER.error("Error occured while creating Account", t);
                context.failNow(t);
            }
        });

        context.awaitCompletion(1, TimeUnit.SECONDS);

        if (createdForUpdate == null) {
            context.failNow(new NullPointerException("Account has not created yet..."));
        }
        createdForUpdate.setAmount(new BigDecimal("10000.00"));
        options.addHeader(SERVICE_OPERATION.key, "update");
        sender.send(address, createdForUpdate.toJson(), options, result -> {
            if (result.succeeded()) {
                Object answer = result.result().body();
                if (answer instanceof JsonObject) {
                    JsonObject json = (JsonObject) answer;
                    try {
                        final int statusCode = json.getInteger("statusCode");
                        assertThat(statusCode).isEqualTo(HttpResponseStatus.OK.code());
                        AccountDataObject updated = new AccountDataObject(json.getJsonObject("result"));
                        assertThat(createdForUpdate).isEqualTo(updated);
                        LOGGER.info("Updated account is " + updated);
                        update.flag();
                    } catch (Throwable e) {
                        context.failNow(e);
                    }
                } else {
                    final String msg = "Usupported object class " + answer.getClass().getName();
                    LOGGER.error(msg);
                    context.failNow(new UnsupportedDataTypeException(msg));
                }
            } else {
                final Throwable t = result.cause();
                LOGGER.error("Error occured while updating Account", t);
                context.failNow(t);
            }
        });
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @Test
    public void updateAccountWithWrongVersionMustBeFail(Vertx vertx, VertxTestContext context) throws InterruptedException {
        Checkpoint create = context.checkpoint();
        Checkpoint update = context.checkpoint();
        createdForUpdate = null;

        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "create");
        AccountDataObject _new = new AccountDataObject();
        sender.send(address, _new.toJson(), options, result -> {
            if (result.succeeded()) {
                Object answer = result.result().body();
                if (answer instanceof JsonObject) {
                    JsonObject json = (JsonObject) answer;
                    try {
                        final int statusCode = json.getInteger("statusCode");
                        assertThat(statusCode).isEqualTo(HttpResponseStatus.CREATED.code());
                        createdForUpdate = new AccountDataObject(json.getJsonObject("result"));
                        assertThat(_new).isEqualTo(createdForUpdate);
                        create.flag();
                    } catch (Throwable e) {
                        context.failNow(e);
                    }
                } else {
                    final String msg = "Usupported object class " + answer.getClass().getName();
                    LOGGER.error(msg);
                    context.failNow(new UnsupportedDataTypeException(msg));
                }
            } else {
                final Throwable t = result.cause();
                LOGGER.error("Error occured while creating Account", t);
                context.failNow(t);
            }
        });

        context.awaitCompletion(1, TimeUnit.SECONDS);

        if (createdForUpdate == null) {
            context.failNow(new NullPointerException("Account has not created yet..."));
        }
        createdForUpdate.setAmount(new BigDecimal("10000.00"));
        options.addHeader(SERVICE_OPERATION.key, "update");
        sender.send(address, createdForUpdate.toJson().put("version", UUID.randomUUID().toString()), options, result -> {
            if (result.succeeded()) {
                Object answer = result.result().body();
                if (answer instanceof JsonObject) {
                    JsonObject json = (JsonObject) answer;
                    try {
                        final int statusCode = json.getInteger("statusCode");
                        assertThat(statusCode).isEqualTo(HttpResponseStatus.OK.code());
                        AccountDataObject updated = new AccountDataObject(json.getJsonObject("result"));
                        assertThat(createdForUpdate).isEqualTo(updated);
                        LOGGER.info("Updated account is " + updated);
                        update.flag();
                    } catch (Throwable e) {
                        context.failNow(e);
                    }
                } else {
                    final String msg = "Usupported object class " + answer.getClass().getName();
                    LOGGER.error(msg);
                    context.failNow(new UnsupportedDataTypeException(msg));
                }
            } else {
                final Throwable t = result.cause();
                LOGGER.error("Error occured while updating Account", t);
                context.failNow(t);
            }
        });
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }
}
