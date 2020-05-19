package de.fraunhofer.fokus.ids.controllers;

import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.managers.DataSourceManager;
import de.fraunhofer.fokus.ids.services.datasourceAdapter.DataSourceAdapterService;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DataSourceController {

    private Logger LOGGER = LoggerFactory.getLogger(DataSourceController.class.getName());
    private DataSourceManager dataSourceManager;
    private DataSourceAdapterService dataSourceAdapterService;

    public DataSourceController(Vertx vertx){
        this.dataSourceManager = new DataSourceManager(vertx);
        this.dataSourceAdapterService = DataSourceAdapterService.createProxy(vertx, Constants.DATASOURCEADAPTER_SERVICE);
    }

    public void getFormSchema(String type, Handler<AsyncResult<JsonObject>> resultHandler) {

        dataSourceAdapterService.getDataSourceFormSchema(type, reply2 -> {
            if (reply2.succeeded()) {
                resultHandler.handle(Future.succeededFuture(reply2.result()));
            }
            else{
                LOGGER.error(reply2.cause());
            }
        });
    }

    public void add(DataSource dataSource, Handler<AsyncResult<JsonObject>> resultHandler) {
        if(dataSource.getDatasourceName() == null || dataSource.getDatasourceName().isEmpty()) {
            JsonObject jO = new JsonObject();
            jO.put("status", "error");
            jO.put("text", "Bitte geben Sie einen DataSourcenamen ein!");
            resultHandler.handle(Future.succeededFuture(jO));
        }
        else if(dataSource.getDatasourceType() == null || dataSource.getDatasourceType() == null) {
            JsonObject jO = new JsonObject();
            jO.put("status", "error");
            jO.put("text", "Bitte geben Sie einen DataSourcetyp ein!");
            resultHandler.handle(Future.succeededFuture(jO));
        }
        else {
            dataSourceManager.add(dataSource, reply -> {
                if(reply.succeeded()) {
                    JsonObject jO = new JsonObject();
                    jO.put("status", "success");
                    jO.put("text", "DataSource wurde erstellt");
                    resultHandler.handle(Future.succeededFuture(jO));
                }
                else {
                    LOGGER.error("DataSource konnte nicht erstellt werden!", reply.cause());
                    JsonObject jO = new JsonObject();
                    jO.put("status", "error");
                    jO.put("text", "DataSource konnte nicht erstellt werden!");
                    resultHandler.handle(Future.succeededFuture(jO));
                }
            });
        }
    }

    public void listAdapters(Handler<AsyncResult<JsonArray>> resultHandler){
        dataSourceAdapterService.listAdapters(jsonArrayAsyncResult -> {
           if (jsonArrayAsyncResult.succeeded()){
               resultHandler.handle(Future.succeededFuture(jsonArrayAsyncResult.result()));
           }
           else {
               resultHandler.handle(Future.failedFuture(jsonArrayAsyncResult.cause()));
           }
        });
    }

    public void delete(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
        dataSourceManager.delete(id, reply -> {
            if(reply.succeeded()) {
                JsonObject jO = new JsonObject();
                jO.put("status", "success");
                jO.put("text", "DataSource wurde gelöscht.");
                resultHandler.handle(Future.succeededFuture(jO));
            }
            else {
                LOGGER.error("DataSource konnte nicht gelöscht werden!", reply.cause());
                JsonObject jO = new JsonObject();
                jO.put("status", "error");
                jO.put("text", "DataSource konnte nicht gelöscht werden!");
                resultHandler.handle(Future.succeededFuture(jO));
            }
        });
    }

    public void update(DataSource dataSource,Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
        dataSource.setId(id);
        dataSourceManager.update(dataSource, reply -> {
            if(reply.succeeded()) {
                JsonObject jO = new JsonObject();
                jO.put("status", "success");
                jO.put("text", "DataSource wurde geändert.");
                resultHandler.handle(Future.succeededFuture(jO));
            }
            else {
                LOGGER.error("DataSource konnte nicht geändert werden!", reply.cause());
                JsonObject jO = new JsonObject();
                jO.put("status", "error");
                jO.put("text", "DataSource konnte nicht geändert werden!");
                resultHandler.handle(Future.succeededFuture(jO));
            }
        });
    }

    public void findAll(Handler<AsyncResult<JsonArray>> resultHandler) {
        dataSourceManager.findAll(reply -> {
            if (reply.succeeded()) {
                resultHandler.handle(Future.succeededFuture(reply.result()));
            }
            else {
                LOGGER.error("DataSources not found.", reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    public void findAllByType(Handler<AsyncResult<JsonObject>> resultHandler) {
        dataSourceManager.findAllByType(reply -> {
            if (reply.succeeded()) {
                resultHandler.handle(Future.succeededFuture(reply.result()));
            }
            else {
                LOGGER.error("DataSources not found.", reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    public void findById(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
        dataSourceManager.findById(id, reply -> {
            if (reply.succeeded()) {

                dataSourceAdapterService.getDataAssetFormSchema(reply.result().getString("datasourcetype"), reply2 -> {
                    if(reply2.succeeded()){

                        JsonObject newjO = new JsonObject()
                                .put("source", reply.result())
                                .put("formSchema", reply2.result());
                        resultHandler.handle(Future.succeededFuture(newjO));
                    }
                        });
            }
            else {
                LOGGER.error("DataSource not found.", reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    public void findByType(String type, Handler<AsyncResult<JsonObject>> resultHandler) {
        dataSourceManager.findByType(type, reply -> {
            if (reply.succeeded()) {
                JsonObject result = new JsonObject().put("type", type).put("result", reply.result());
                resultHandler.handle(Future.succeededFuture(result));
            }
            else {
                LOGGER.error("DataSources not found.", reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }
}
