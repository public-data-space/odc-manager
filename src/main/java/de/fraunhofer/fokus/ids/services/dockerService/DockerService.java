package de.fraunhofer.fokus.ids.services.dockerService;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;

import javax.xml.crypto.Data;

@ProxyGen
@VertxGen
public interface DockerService {

    @Fluent
    DockerService getImages(Handler<AsyncResult<JsonArray>> resultHandler);

    @Fluent
    DockerService startImages(String uuid, Handler<AsyncResult<JsonObject>> resultHandler);

    @Fluent
    DockerService stopImages(String uuid, Handler<AsyncResult<JsonObject>> resultHandler);

    @GenIgnore
    static DockerService create(Vertx vertx, WebClient webClient, int gatewayPort, String gatewayHost, String tempFileRootPath, Handler<AsyncResult<DockerService>> readyHandler) {
        return new DockerServiceImpl(vertx, webClient, gatewayPort, gatewayHost, tempFileRootPath, readyHandler);
    }

    @GenIgnore
    static DockerService createProxy(Vertx vertx, String address) {
        return new DockerServiceVertxEBProxy(vertx, address);
    }

}
