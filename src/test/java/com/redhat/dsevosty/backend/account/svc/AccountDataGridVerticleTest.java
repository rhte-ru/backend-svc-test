package com.redhat.dsevosty.backend.account.svc;

import static org.assertj.core.api.Assertions.assertThat;
import static com.redhat.dsevosty.common.ServiceConstant.*;

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

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
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

    @BeforeAll
    public static void setUp(Vertx vertx, VertxTestContext context) throws InterruptedException {
        HotRodServerConfigurationBuilder serverConfig = new HotRodServerConfigurationBuilder()
                .host(SERVICE_JDG_REMOTE_ADDRESS.value).defaultCacheName(PUBLIC_CONTEXT_NAME)
                .port(Integer.valueOf(SERVICE_JDG_REMOTE_PORT.value));
        ConfigurationBuilder cacheConfig = new ConfigurationBuilder();
        server = new InfinispanLocalHotrodServer<UUID, AbstractDataObject>(cacheConfig.build(), serverConfig.build());

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
        sender = vertx.eventBus();
        LOGGER.trace("BeforeEach created client {}", sender);
        address = SERVICE_EVENTBUS_PREFIX.value + "." + PUBLIC_CONTEXT_NAME;
        context.completeNow();
    }

    @AfterEach
    public void after(Vertx vertx, VertxTestContext context) {
        LOGGER.trace("AfterEach closing client {}", sender);
        sender.close(result -> {
            context.completeNow();
        });
        context.completeNow();
    }

    @Test
    public void createAccount(Vertx vertx, VertxTestContext context) throws InterruptedException {
        DeliveryOptions options = new DeliveryOptions();
        options.addHeader(SERVICE_OPERATION.key, "add");
        AccountDataObject ado = new AccountDataObject();
        sender.send(address, ado.toJson(), options, result -> {
            if (result.succeeded()) {
                Object answer = result.result().body();
                // LOGGER.info("Account {} added", answer);
                if (answer instanceof JsonObject) {
                    JsonObject json = (JsonObject) answer;
                    assertThat(json.getInteger("statusCode")).isEqualTo(201);
                    LOGGER.info("Account {} added", json.getString("result"));
                    context.completeNow();
                } else {
                    final String msg = "Usupported object class " + answer.getClass().getName();
                    LOGGER.error(msg);
                    context.failNow(new UnsupportedDataTypeException(msg));
                }
            } else {
                LOGGER.error("Error occured while creating Account: " + result.cause());
                context.failNow(result.cause());
            }
        });
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }
}