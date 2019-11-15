package de.fraunhofer.fokus.ids.persistence.managers;

import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.persistence.service.DatabaseService;
import de.fraunhofer.fokus.ids.persistence.util.BrokerStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.time.Instant;
import java.util.Date;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class BrokerManager {

    private Logger LOGGER = LoggerFactory.getLogger(DataAssetManager.class.getName());
    private DatabaseService dbService;

    private static final String UPDATE_QUERY = "INSERT INTO Broker (created_at, updated_at, url, status) values (NOW(), NOW(), ?, ?)";
    private static final String UNREGISTER_QUERY =  "Update Broker SET updated_at = NOW(), status = ?  WHERE id = ?";
    private static final String UNREGISTERBYURL_QUERY =  "Update Broker SET updated_at = NOW(), status = ?  WHERE url = ?";
    private static final String REGISTER_QUERY =  "Update Broker SET updated_at = NOW(), status = ?  WHERE id = ?";
    private static final String FINDALL_QUERY = "SELECT * FROM Broker";
    private static final String FINDBYID_QUERY = "SELECT * FROM Broker WHERE id = ?";
    private static final String FINDBYCREATE_QUERY = "SELECT * FROM Broker WHERE created_at = ?";
    private static final String DELETE_QUERY = "DELETE FROM Broker WHERE id = ?";

    public BrokerManager(Vertx vertx) {
        dbService = DatabaseService.createProxy(vertx, Constants.DATABASE_SERVICE);
    }

    public void add(String url, Handler<AsyncResult<Void>> resultHandler){
        JsonArray params = new JsonArray()
                .add(url)
                .add(BrokerStatus.REGISTERED);

        dbService.update(UPDATE_QUERY, params, reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        });
    }

    public void unregister(long id, Handler<AsyncResult<JsonObject>> resultHandler){
        JsonArray params = new JsonArray()
                .add(BrokerStatus.UNREGISTERED)
                .add(id);

        performUnregister(UNREGISTER_QUERY, params, resultHandler);

    }

    public void delete(long id, Handler<AsyncResult<JsonObject>> resultHandler){
        JsonArray params = new JsonArray()
                .add(id);

        dbService.update(DELETE_QUERY, params, reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        });
    }

    public void unregisterByUrl(String url, Handler<AsyncResult<JsonObject>> resultHandler){
        JsonArray params = new JsonArray()
                .add(BrokerStatus.UNREGISTERED)
                .add(url);

        performUnregister(UNREGISTERBYURL_QUERY, params, resultHandler);
    }

    private void performUnregister(String query, JsonArray params, Handler<AsyncResult<JsonObject>> resultHandler ){
        dbService.update(query, params, reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        });
    }

    public void register(long id, Handler<AsyncResult<JsonObject>> resultHandler){
        JsonArray params = new JsonArray()
                .add(BrokerStatus.REGISTERED)
                .add(id);

        dbService.update(REGISTER_QUERY, params, reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        });
    }

    public void findAll(Handler<AsyncResult<JsonArray>> resultHandler){
        dbService.query(FINDALL_QUERY, new JsonArray(), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            } else {
                resultHandler.handle(Future.succeededFuture(new JsonArray(reply.result())));
            }
        });
    }

    public void findById(long id, Handler<AsyncResult<JsonObject>> resultHandler){
        dbService.query(FINDBYID_QUERY, new JsonArray().add(id), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            } else {
                resultHandler.handle(Future.succeededFuture(reply.result().get(0)));
            }
        });
    }

}
