package de.fraunhofer.fokus.ids.services.webclient;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.web.client.WebClient;

import java.util.List;

public class WebClientServiceImpl implements WebClientService {

    private Logger LOGGER = LoggerFactory.getLogger(WebClientServiceImpl.class.getName());

    private WebClient webClient;

    public WebClientServiceImpl(WebClient webClient, Handler<AsyncResult<WebClientService>> readyHandler) {
        this.webClient = webClient;
        readyHandler.handle(Future.succeededFuture(this));
    }

    @Override
    public WebClientService post(int port, String host, String path, JsonObject payload, Handler<AsyncResult<JsonObject>> resultHandler) {
        webClient
                .post(port, host, path)
                .sendJsonObject(payload, ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar.result().bodyAsJsonObject()));
                    } else {
                        LOGGER.error("No response from CKAN.\n\n" + ar.cause().getMessage());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
        return this;
    }

    @Override
    public WebClientService get(int port, String host, String path, Handler<AsyncResult<JsonObject>> resultHandler) {

        webClient
                .get(port, host, path)
                .send(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar.result().bodyAsJsonObject()));
                    } else {
                        LOGGER.error("No response from CKAN.\n\n" + ar.cause().getMessage());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
        return this;
    }
}
