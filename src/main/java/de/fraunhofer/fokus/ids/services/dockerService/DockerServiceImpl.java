package de.fraunhofer.fokus.ids.services.dockerService;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.WebClient;

public class DockerServiceImpl implements DockerService {
    private Logger LOGGER = LoggerFactory.getLogger(DockerServiceImpl.class.getName());

    private WebClient webClient;
    private int configManagerPort;
    private String configManagerHost;
    private String configManagerApikey;

    public DockerServiceImpl(WebClient webClient, int gatewayPort, String gatewayHost, String configManagerApikey, Handler<AsyncResult<DockerService>> readyHandler) {
        this.webClient = webClient;
        this.configManagerHost = gatewayHost;
        this.configManagerPort = gatewayPort;
        this.configManagerApikey = configManagerApikey;

        readyHandler.handle(Future.succeededFuture(this));
    }
    private void post(int port, String host, String path, String payload, Handler<AsyncResult<JsonArray>> resultHandler) {
        webClient
                .post(port, host, path)
                .sendJson(payload, ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar.result().bodyAsJsonArray()));
                    } else {
                        LOGGER.error(ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }
    private void get(int port, String host, String path, Handler<AsyncResult<JsonArray>> resultHandler) {

        webClient
                .get(port, host, path)
                .bearerTokenAuthentication(configManagerApikey)
                .send(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar.result().bodyAsJsonArray()));
                    } else {
                        LOGGER.error(ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    @Override
    public DockerService getImages(Handler<AsyncResult<JsonArray>> resultHandler) {
        get(configManagerPort, configManagerHost,"/images/",jsonArrayAsyncResult -> {
            if (jsonArrayAsyncResult.succeeded()){
                resultHandler.handle(Future.succeededFuture(jsonArrayAsyncResult.result()));
            }
            else {
                LOGGER.error(jsonArrayAsyncResult.cause());
                resultHandler.handle(Future.failedFuture(jsonArrayAsyncResult.cause()));
            }
        });
        return this;
    }

    @Override
    public DockerService startImages(String uuid, Handler<AsyncResult<JsonObject>> resultHandler) {
        webClient
                .post(configManagerPort, configManagerHost, "/images/start/")
                .bearerTokenAuthentication(configManagerApikey)
                .sendBuffer(Buffer.buffer(uuid), ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar.result().bodyAsJsonObject()));
                    } else {
                        LOGGER.error(ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
        return this;
    }

    @Override
    public DockerService stopImages(String uuid, Handler<AsyncResult<JsonObject>> resultHandler) {
        webClient
                .post(configManagerPort, configManagerHost, "/images/stop/")
                .bearerTokenAuthentication(configManagerApikey)
                .sendBuffer(Buffer.buffer(uuid), ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar.result().bodyAsJsonObject()));
                    } else {
                        LOGGER.error(ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
        return this;
    }

    @Override
    public DockerService getDataAssetFormSchema(String dataSourceType, Handler<AsyncResult<JsonObject>> resultHandler) {
        webClient
                .get(configManagerPort, configManagerHost, "/getAdapter/"+dataSourceType)
                .bearerTokenAuthentication(configManagerApikey)
                .send(reply -> {
                    if(reply.succeeded()) {
                        webClient
                                .get(reply.result().bodyAsJsonObject().getInteger("port"), reply.result().bodyAsJsonObject().getString("host"), "/getDataAssetFormSchema/")
                                .bearerTokenAuthentication(configManagerApikey)
                                .send(adapterReply -> {
                                    if (adapterReply.succeeded()) {
                                        resultHandler.handle(Future.succeededFuture(adapterReply.result().bodyAsJsonObject()));
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
    public DockerService getDataSourceFormSchema(String dataSourceType,Handler<AsyncResult<JsonObject>> resultHandler) {
        webClient
                .get(configManagerPort, configManagerHost, "/getAdapter/"+dataSourceType)
                .bearerTokenAuthentication(configManagerApikey)
                .send(reply -> {
            if(reply.succeeded()) {
                webClient
                        .get(reply.result().bodyAsJsonObject().getInteger("port"), reply.result().bodyAsJsonObject().getString("host"), "/getDataSourceFormSchema/")
                        .bearerTokenAuthentication(configManagerApikey)
                        .send(adapterReply -> {
                            if (adapterReply.succeeded()) {
                                resultHandler.handle(Future.succeededFuture(adapterReply.result().bodyAsJsonObject()));
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
    public DockerService listAdapters(Handler<AsyncResult<JsonArray>> resultHandler) {
        webClient
                .get(configManagerPort, configManagerHost, "/listAdapters/")
                .bearerTokenAuthentication(configManagerApikey)
                .send(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar.result().bodyAsJsonArray()));
                    } else {
                        LOGGER.error(ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
        return this;
    }
}
