package de.fraunhofer.fokus.ids.services.datasourceAdapter;

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
public interface DataSourceAdapterService {

    @Fluent
    DataSourceAdapterService getFile(String dataSourceType, JsonObject request, Handler<AsyncResult<JsonObject>> resultHandler);

    @Fluent
    DataSourceAdapterService supported(String dataSourceType, Handler<AsyncResult<JsonObject>> resultHandler);

    @Fluent
    DataSourceAdapterService delete(String dataSourceType, Long id, Handler<AsyncResult<JsonObject>> resultHandler);

    @Fluent
    DataSourceAdapterService createDataAsset(String dataSourceType, JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler);


    @GenIgnore
    static DataSourceAdapterService create(WebClient webClient, Handler<AsyncResult<DataSourceAdapterService>> readyHandler) {
        return new DataSourceAdapterServiceImpl(webClient, readyHandler);
    }

    @GenIgnore
    static DataSourceAdapterService createProxy(Vertx vertx, String address) {
        return new DataSourceAdapterServiceVertxEBProxy(vertx, address);
    }

}
