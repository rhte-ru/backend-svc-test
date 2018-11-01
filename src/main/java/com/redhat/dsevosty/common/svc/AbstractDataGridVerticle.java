package com.redhat.dsevosty.common.svc;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public abstract class AbstractDataGridVerticle extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDataGridVerticle.class);

    @Override 
    public void stop(Future<Void> stop) {
        LOGGER.trace("About to stop Verticle");

    }
    
    @Override
    public void start(Future<Void> start) {
        LOGGER.trace("About to stop Verticle");
    }

    // public abstract
}