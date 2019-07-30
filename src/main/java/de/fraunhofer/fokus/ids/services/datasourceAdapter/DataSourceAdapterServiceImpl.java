package de.fraunhofer.fokus.ids.services.datasourceAdapter;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.WebClient;

public class DataSourceAdapterServiceImpl implements DataSourceAdapterService {

    private Logger LOGGER = LoggerFactory.getLogger(DataSourceAdapterServiceImpl.class.getName());

    private WebClient webClient;

    public DataSourceAdapterServiceImpl(WebClient webClient, Handler<AsyncResult<DataSourceAdapterService>> readyHandler) {
        this.webClient = webClient;
        readyHandler.handle(Future.succeededFuture(this));
    }

    private void post(int port, String host, String path, JsonObject payload, Handler<AsyncResult<JsonObject>> resultHandler) {
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
    }

    private void get(int port, String host, String path, Handler<AsyncResult<JsonObject>> resultHandler) {

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
    }

    @Override
    public DataSourceAdapterService getFile(String dataSourceType, JsonObject request, Handler<AsyncResult<JsonObject>> resultHandler) {

        post(8093, "localhost", "/getFile/"+dataSourceType, request, reply -> {
            if (reply.succeeded()) {
                resultHandler.handle(Future.succeededFuture(reply.result()));
            } else {
                LOGGER.error("No response from CKAN.\n\n" + reply.cause().getMessage());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
        return this;
    }

    @Override
    public DataSourceAdapterService supported(String dataSourceType, Handler<AsyncResult<JsonObject>> resultHandler) {
        get(8093, "localhost", "/supported/"+dataSourceType, reply -> {
            if (reply.succeeded()) {
                resultHandler.handle(Future.succeededFuture(reply.result()));
            } else {
                LOGGER.error("No response from CKAN.\n\n" + reply.cause().getMessage());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
        return this;
    }

    @Override
    public DataSourceAdapterService delete(String dataSourceType, Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
        get(8093, "localhost", "/supported/"+dataSourceType+"/"+id, reply -> {
            if (reply.succeeded()) {
                resultHandler.handle(Future.succeededFuture(reply.result()));
            } else {
                LOGGER.error("No response from CKAN.\n\n" + reply.cause().getMessage());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
        return this;
    }

    @Override
    public DataSourceAdapterService createDataAsset(String dataSourceType, JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler) {
        post(8093, "localhost", "/create/"+dataSourceType, message, reply -> {
            if (reply.succeeded()) {
                resultHandler.handle(Future.succeededFuture(reply.result()));
            } else {
                LOGGER.error("No response from CKAN.\n\n" + reply.cause().getMessage());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
        return this;
    }
}
