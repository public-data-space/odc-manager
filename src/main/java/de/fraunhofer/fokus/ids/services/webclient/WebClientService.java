package de.fraunhofer.fokus.ids.services.webclient;


import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;

@ProxyGen
@VertxGen
public interface WebClientService {

    @Fluent
    WebClientService post(int port, String host, String path, JsonObject payload, Handler<AsyncResult<JsonObject>> resultHandler);

    @Fluent
    WebClientService get(int port, String host, String path, Handler<AsyncResult<JsonObject>> resultHandler);

    @GenIgnore
    static WebClientService create(WebClient webClient, Handler<AsyncResult<WebClientService>> readyHandler) {
        return new WebClientServiceImpl(webClient, readyHandler);
    }

    @GenIgnore
    static WebClientService createProxy(Vertx vertx, String address) {
        return new WebClientServiceVertxEBProxy(vertx, address);
    }

}
