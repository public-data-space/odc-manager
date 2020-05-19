package de.fraunhofer.fokus.ids.controllers;

import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.services.dockerService.DockerService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class DockerController {
    private Logger LOGGER = LoggerFactory.getLogger(DockerController.class.getName());
    private DockerService dockerService;
    public DockerController(Vertx vertx){
        this.dockerService = DockerService.createProxy(vertx, Constants.DOCKER_SERVICE);
    }

    public void getImages(Handler<AsyncResult<JsonArray>> resultHandler){
        dockerService.getImages(jsonArrayAsyncResult -> {
            if (jsonArrayAsyncResult.succeeded()){
                resultHandler.handle(Future.succeededFuture(jsonArrayAsyncResult.result()));
            }
            else {
                resultHandler.handle(Future.failedFuture(jsonArrayAsyncResult.cause()));
            }
        });
    }

    public void startImages(String uuid, Handler<AsyncResult<JsonObject>> resultHandler){
        dockerService.startImages(uuid,jsonObjectAsyncResult -> {
            if (jsonObjectAsyncResult.succeeded()){
                resultHandler.handle(Future.succeededFuture(jsonObjectAsyncResult.result()));
            }
            else {
                resultHandler.handle(Future.failedFuture(jsonObjectAsyncResult.cause()));
            }
        });
    }

    public void stopImages(String uuid, Handler<AsyncResult<JsonObject>> resultHandler){
        dockerService.stopImages(uuid,jsonObjectAsyncResult -> {
            if (jsonObjectAsyncResult.succeeded()){
                resultHandler.handle(Future.succeededFuture(jsonObjectAsyncResult.result()));
            }
            else {
                resultHandler.handle(Future.failedFuture(jsonObjectAsyncResult.cause()));
            }
        });
    }
}
