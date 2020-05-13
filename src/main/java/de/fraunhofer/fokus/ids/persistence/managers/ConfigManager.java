package de.fraunhofer.fokus.ids.persistence.managers;

import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.persistence.service.DatabaseService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class ConfigManager {
    private Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class.getName());

    private DatabaseService dbService;

    public ConfigManager(Vertx vertx){
        this.dbService = DatabaseService.createProxy(vertx, Constants.DATABASE_SERVICE);
    }

    public void getConfiguration(Handler<AsyncResult<JsonObject>> resultHandler){
        dbService.query("SELECT * FROM configuration", new JsonArray(), reply -> {
            if(reply.succeeded()){
                if(reply.result().size()>0) {
                    resultHandler.handle(Future.succeededFuture(reply.result().get(0)));
                }
                else{
                    LOGGER.error("No configuration available.");
                    resultHandler.handle(Future.failedFuture("No configuration available."));
                }
            }
            else{
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    public void editConfiguration(JsonObject json, Handler<AsyncResult<JsonObject>> resultHandler){
        JsonArray params = new JsonArray()
                .add(json.getString("title"))
                .add(json.getString("maintainer"))
                .add(json.getString("curator"))
                .add(json.getString("url"))
                .add(json.getString("country"));
        getConfiguration(reply -> {
            if(reply.succeeded()){
                params.add(reply.result().getLong("id"));
                dbService.query("UPDATE configuration SET title = ?, maintainer = ?, curator = ?, url = ?, country = ? WHERE id = ?", params, reply2 -> {
                    if(reply2.succeeded()){
                        JsonObject jO = new JsonObject();
                        jO.put("status", "success");
                        jO.put("text", "Konfiguration geändert");
                        resultHandler.handle(Future.succeededFuture(jO));
                    }
                    else{
                        LOGGER.error(reply2.cause());
                        resultHandler.handle(Future.failedFuture(reply.cause()));
                    }
                });
            } else{
                dbService.query("INSERT INTO configuration (title, maintainer, curator, url, country) values (?,?,?,?,?)", params, reply2 -> {
                    if(reply2.succeeded()){
                        JsonObject jO = new JsonObject();
                        jO.put("status", "success");
                        jO.put("text", "Konfiguration geändert");
                        resultHandler.handle(Future.succeededFuture(jO));
                    }
                    else{
                        LOGGER.error(reply2.cause());
                        resultHandler.handle(Future.failedFuture(reply2.cause()));
                    }
                });
            }
        });
    }

}
