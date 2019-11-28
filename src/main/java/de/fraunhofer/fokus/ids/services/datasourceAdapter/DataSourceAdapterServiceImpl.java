package de.fraunhofer.fokus.ids.services.datasourceAdapter;

import de.fraunhofer.fokus.ids.models.FileResponse;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DataSourceAdapterServiceImpl implements DataSourceAdapterService {

    private Logger LOGGER = LoggerFactory.getLogger(DataSourceAdapterServiceImpl.class.getName());

    private WebClient webClient;
    private int configManagerPort;
    private String configManagerHost;

    public DataSourceAdapterServiceImpl(WebClient webClient, int gatewayPort, String gatewayHost, Handler<AsyncResult<DataSourceAdapterService>> readyHandler) {
        this.webClient = webClient;
        this.configManagerHost = gatewayHost;
        this.configManagerPort = gatewayPort;

        readyHandler.handle(Future.succeededFuture(this));
    }

    private void post(int port, String host, String path, JsonObject payload, Handler<AsyncResult<JsonObject>> resultHandler) {
        webClient
                .post(port, host, path)
                .sendJsonObject(payload, ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar.result().bodyAsJsonObject()));
                    } else {
                        LOGGER.error(ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    private void download(int port, String host, String path, JsonObject payload, Handler<AsyncResult<JsonObject>> resultHandler) {
        webClient
                .post(port, host, path)
                .sendJsonObject(payload, ar -> {
                    if (ar.succeeded()) {
                        FileResponse fileResponse = new FileResponse();
                        Map<String, List<String>> headers = new HashMap<>();
                        for(String name : ar.result().headers().names()){
                            headers.put(name, ar.result().headers().getAll(name));
                        }
                        LOGGER.info(ar.result().bodyAsString());
                        fileResponse.setHeaders(headers);
                        fileResponse.setBody(ar.result().bodyAsString());
                        resultHandler.handle(Future.succeededFuture(new JsonObject(Json.encode(fileResponse))));
                    } else {
                        LOGGER.error(ar.cause());
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
                        LOGGER.error(ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    @Override
    public DataSourceAdapterService getFile(String dataSourceType, JsonObject request, Handler<AsyncResult<JsonObject>> resultHandler) {
        get(configManagerPort, configManagerHost,"/getAdapter/"+dataSourceType, reply -> {
            if(reply.succeeded()) {
                download(reply.result().getInteger("port"), reply.result().getString("host"), "/getFile/", request, adapterReply -> {
                    if (adapterReply.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(adapterReply.result()));
                    } else {
                        LOGGER.error(adapterReply.cause());
                        resultHandler.handle(Future.failedFuture(adapterReply.cause()));
                    }
                });
            } else {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
        return this;
    }

    @Override
    public DataSourceAdapterService supported(String dataSourceType, Handler<AsyncResult<JsonObject>> resultHandler) {
        get(configManagerPort, configManagerHost,"/getAdapter/"+dataSourceType, reply -> {
            if(reply.succeeded()) {
                get(reply.result().getInteger("port"), reply.result().getString("host"), "/supported/", adapterReply -> {
                    if (adapterReply.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(adapterReply.result()));
                    } else {
                        LOGGER.error(adapterReply.cause());
                        resultHandler.handle(Future.failedFuture(adapterReply.cause()));
                    }
                });
            } else {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
        return this;
    }

    @Override
    public DataSourceAdapterService delete(String dataSourceType, Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
        get(configManagerPort, configManagerHost,"/getAdapter/"+dataSourceType, reply -> {
            if(reply.succeeded()) {
                get(reply.result().getInteger("port"), reply.result().getString("host"), "/delete/", adapterReply -> {
                    if (adapterReply.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(adapterReply.result()));
                    } else {
                        LOGGER.error(adapterReply.cause());
                        resultHandler.handle(Future.failedFuture(adapterReply.cause()));
                    }
                });
            } else {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
        return this;
    }

    @Override
    public DataSourceAdapterService createDataAsset(String dataSourceType, JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler) {
        get(configManagerPort, configManagerHost,"/getAdapter/"+dataSourceType, reply -> {
            if(reply.succeeded()) {
                post(reply.result().getInteger("port"), reply.result().getString("host"), "/create/", message, adapterReply -> {
                    if (adapterReply.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(adapterReply.result()));
                    } else {
                        LOGGER.error(adapterReply.cause());
                        resultHandler.handle(Future.failedFuture(adapterReply.cause()));
                    }
                });
            } else {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
        return this;
    }

    @Override
    public DataSourceAdapterService getDataAssetFormSchema(String dataSourceType, Handler<AsyncResult<JsonObject>> resultHandler) {
        LOGGER.info(dataSourceType);
        get(configManagerPort, configManagerHost,"/getAdapter/"+dataSourceType, reply -> {
            if(reply.succeeded()) {
                get(reply.result().getInteger("port"), reply.result().getString("host"), "/getDataAssetFormSchema/", adapterReply -> {
                    if (adapterReply.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(adapterReply.result()));
                    } else {
                        LOGGER.error(adapterReply.cause());
                        resultHandler.handle(Future.failedFuture(adapterReply.cause()));
                    }
                });
            } else {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
        return this;
    }

    @Override
    public DataSourceAdapterService getDataSourceFormSchema(String dataSourceType,Handler<AsyncResult<JsonObject>> resultHandler) {
        LOGGER.info(dataSourceType);
        get(configManagerPort, configManagerHost,"/getAdapter/"+dataSourceType, reply -> {
            if(reply.succeeded()) {
                get(reply.result().getInteger("port"), reply.result().getString("host"), "/getDataSourceFormSchema/", adapterReply -> {
                    if (adapterReply.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(adapterReply.result()));
                    } else {
                        LOGGER.error(adapterReply.cause());
                        resultHandler.handle(Future.failedFuture(adapterReply.cause()));
                    }
                });
            } else {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
        return this;
    }
}
