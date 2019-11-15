package de.fraunhofer.fokus.ids.persistence.managers;

import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.service.DatabaseService;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static de.fraunhofer.fokus.ids.persistence.util.Functions.checkNull;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DataSourceManager {

    private DatabaseService dbService;
    private Logger LOGGER = LoggerFactory.getLogger(DataSourceManager.class.getName());

    private static final String UPDATE_QUERY = "UPDATE DataSource SET updated_at = NOW(), datasourcename = ?, data = ?, datasourcetype = ? WHERE id = ?";
    private static final String ADD_QUERY = "INSERT INTO DataSource (created_at, updated_at, datasourcename, data, datasourcetype) values (NOW(), NOW(), ?, ?::JSON, ?)";
    private static final String DELETE_QUERY = "DELETE FROM datasource WHERE id = ?";
    private static final String FINDBYTYPE_QUERY = "SELECT * FROM DataSource WHERE datasourcetype = ?";
    private static final String FINDBYID_QUERY = "SELECT * FROM DataSource WHERE id = ?";
    private static final String FINDALL_QUERY ="SELECT * FROM DataSource ORDER BY id DESC";
    private static final String FINDTYPEBYID_QUERY = "SELECT datasourcetype FROM DataSource WHERE id = ?";
    private static final String FINDALLBYTYPE_QUERY = "SELECT * FROM DataSource ORDER BY datasourcetype";

    public DataSourceManager(Vertx vertx) {
        dbService = DatabaseService.createProxy(vertx, Constants.DATABASE_SERVICE);
    }

    public void update(DataSource dataSource, Handler<AsyncResult<Void>> resultHandler) {

        JsonArray params = new JsonArray()
                .add(checkNull(dataSource.getDatasourceName()))
                .add(dataSource.getData())
                .add(dataSource.getDatasourceType())
                .add(dataSource.getId());

        dbService.update(UPDATE_QUERY, params, reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        });

    }

    public void add(DataSource dataSource, Handler<AsyncResult<Void>> resultHandler) {

        JsonArray params = new JsonArray()
                .add(checkNull(dataSource.getDatasourceName()))
                .add(dataSource.getData().toString())
                .add(dataSource.getDatasourceType());

        dbService.update(ADD_QUERY, params, reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        });
    }

    public void delete(Long id, Handler<AsyncResult<Void>> resultHandler) {
        dbService.update(DELETE_QUERY,new JsonArray().add(id), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        });
    }

    public void findByType(String type, Handler<AsyncResult<JsonArray>> resultHandler) {
        dbService.query(FINDBYTYPE_QUERY,new JsonArray().add(type), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                resultHandler.handle(Future.succeededFuture(new JsonArray(reply.result())));
            }
        });
    }

    public void findById(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
        dbService.query(FINDBYID_QUERY,new JsonArray().add(id), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                resultHandler.handle(Future.succeededFuture(reply.result().get(0)));
            }
        });
    }

    public void findAll(Handler<AsyncResult<JsonArray>> resultHandler) {
        dbService.query(FINDALL_QUERY ,new JsonArray(), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                resultHandler.handle(Future.succeededFuture(new JsonArray(reply.result())));
            }
        });
    }

    public void findTypeById(Long id, Handler<AsyncResult<Long>> resultHandler) {
        dbService.query(FINDTYPEBYID_QUERY, new JsonArray().add(id), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                resultHandler.handle(Future.succeededFuture(reply.result().get(0).getLong("datasourcetype")));
            }
        });
    }

    public void findAllByType(Handler<AsyncResult<JsonObject>> resultHandler) {
        dbService.query( FINDALLBYTYPE_QUERY ,new JsonArray(), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {

                JsonObject res = new JsonObject();
                for(JsonObject jsonObject : reply.result()){
                    if(!res.containsKey(jsonObject.getString("datasourcetype"))){
                        res.put(jsonObject.getString("datasourcetype"), new JsonArray());
                    }
                    res.getJsonArray(jsonObject.getString("datasourcetype")).add(jsonObject);
                }
                resultHandler.handle(Future.succeededFuture(res));
            }
        });
    }

}