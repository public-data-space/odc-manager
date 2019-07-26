package de.fraunhofer.fokus.ids.controllers;

import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.enums.DatasourceType;
import de.fraunhofer.fokus.ids.persistence.managers.DataSourceManager;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class DataSourceController {

    private Logger LOGGER = LoggerFactory.getLogger(DataSourceController.class.getName());
    private DataSourceManager dataSourceManager;

    public DataSourceController(Vertx vertx){
        this.dataSourceManager = new DataSourceManager(vertx);
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
                    LOGGER.error("DataSource konnte nicht erstellt werden!\n\n"+reply.cause());
                    JsonObject jO = new JsonObject();
                    jO.put("status", "error");
                    jO.put("text", "DataSource konnte nicht erstellt werden!");
                    resultHandler.handle(Future.succeededFuture(jO));
                }
            });
        }
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
                LOGGER.error("DataSource konnte nicht gelöscht werden!\n\n"+reply.cause());
                JsonObject jO = new JsonObject();
                jO.put("status", "error");
                jO.put("text", "DataSource konnte nicht gelöscht werden!");
                resultHandler.handle(Future.succeededFuture(jO));
            }
        });
    }

    public void update(DataSource dataSource, Handler<AsyncResult<JsonObject>> resultHandler) {
        dataSourceManager.update(dataSource, reply -> {
            if(reply.succeeded()) {
                JsonObject jO = new JsonObject();
                jO.put("status", "success");
                jO.put("text", "DataSource wurde geändert.");
                resultHandler.handle(Future.succeededFuture(jO));
            }
            else {
                LOGGER.error("DataSource konnte nicht geändert werden!\n\n"+reply.cause());
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
                LOGGER.error("DataSources not found. Cause: "+ reply.cause());
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
                LOGGER.error("DataSources not found. Cause: "+ reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    public void findById(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
        dataSourceManager.findById(id, reply -> {
            if (reply.succeeded()) {
                resultHandler.handle(Future.succeededFuture(reply.result()));
            }
            else {
                LOGGER.error("DataSource not found. Cause: "+ reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    public void findByType(DatasourceType type, Handler<AsyncResult<JsonArray>> resultHandler) {
        dataSourceManager.findByType(type, reply -> {
            if (reply.succeeded()) {
                resultHandler.handle(Future.succeededFuture(reply.result()));
            }
            else {
                LOGGER.error("DataSources not found. Cause: "+ reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }
}
