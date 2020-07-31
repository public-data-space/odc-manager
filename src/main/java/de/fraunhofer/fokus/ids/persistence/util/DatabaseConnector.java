package de.fraunhofer.fokus.ids.persistence.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.*;

import java.util.List;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DatabaseConnector {

    private Logger LOGGER = LoggerFactory.getLogger(DatabaseConnector.class.getName());
    private PgPool client;
    private RowTransformer rowTransformer;
    private static final DatabaseConnector DBC = new DatabaseConnector();

    private DatabaseConnector() {
        this.rowTransformer = new RowTransformer();
    }
    public static DatabaseConnector getInstance() {
        return DBC;
    }

    public void create(Vertx vertx, JsonObject config, int maxPoolSize){
        if(client == null) {
            PgConnectOptions connectOptions = new PgConnectOptions()
                    .setPort(config.getInteger("port"))
                    .setHost(config.getString("host"))
                    .setDatabase(config.getString("database"))
                    .setUser(config.getString("user"))
                    .setPassword(config.getString("password"));

            PoolOptions poolOptions = new PoolOptions()
                    .setMaxSize(maxPoolSize);

            this.client = PgPool.pool(vertx, connectOptions, poolOptions);
        } else {
            LOGGER.info("Client already initialized.");
        }
    }

    public void query(String query, Tuple params, Handler<AsyncResult<List<JsonObject>>> resultHandler){
        client.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                SqlConnection conn = ar1.result();
                conn.preparedQuery(query)
                        .execute(params, ar2 -> {
                            if (ar2.succeeded()) {
                                resultHandler.handle(Future.succeededFuture(rowTransformer.transform(ar2.result())));
                                conn.close();
                            } else {
                                conn.close();
                                LOGGER.error(ar2.cause());
                                resultHandler.handle(Future.failedFuture(ar2.cause()));
                            }
                        });
            } else {
                LOGGER.error(ar1.cause());
                resultHandler.handle(Future.failedFuture(ar1.cause()));
            }
        });
    }
}
