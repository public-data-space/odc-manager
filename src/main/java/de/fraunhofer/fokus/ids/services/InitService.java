package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.persistence.util.DatabaseConnector;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.mindrot.jbcrypt.BCrypt;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class InitService{

	private final Logger LOGGER = LoggerFactory.getLogger(InitService.class.getName());

	private Vertx vertx;

	private final String ADMIN_CREATE_QUERY = "INSERT INTO public.user(created_at, updated_at, username, password) SELECT NOW(), NOW(), $1, $2 WHERE NOT EXISTS ( SELECT 1 FROM public.user WHERE username=$1)";
	private final String DATASOURCEFILEUPLOAD_CREATE_QUERY = "INSERT INTO datasource(created_at, updated_at, datasourcename,data, datasourcetype) SELECT NOW(), NOW(), $1, $2 ,$3 WHERE NOT EXISTS ( SELECT 1 FROM datasource WHERE datasourcetype=$3)";

	private final JsonObject user = new JsonObject().put("id","SERIAL")
			.put("created_at","TIMESTAMP")
			.put("updated_at","TIMESTAMP")
			.put("username","TEXT")
			.put("password","TEXT");

	private final JsonObject dataasset = new JsonObject().put("id","SERIAL").put("created_at","TIMESTAMP")
			.put("updated_at","TIMESTAMP").put("datasetid","TEXT").put("name","TEXT")
			.put("url","TEXT").put("format","TEXT").put("licenseurl","TEXT").put("licensetitle","TEXT").put("datasettitle","TEXT")
			.put("datasetnotes","TEXT").put("orignalresourceurl","TEXT")
			.put("orignaldataseturl","TEXT").put("signature","TEXT").put("status","INTEGER").put("resourceid","TEXT")
			.put("tags","TEXT[]").put("datasetdescription","TEXT").put("organizationtitle","TEXT")
			.put("organizationdescription","TEXT").put("version","TEXT").put("organizationdescription","TEXT")
			.put("sourceid","TEXT");

	private final JsonObject datasource = new JsonObject().put("id","SERIAL")
			.put("created_at","TIMESTAMP")
			.put("updated_at","TIMESTAMP")
			.put("datasourcename","TEXT")
			.put("data","JSONB")
			.put("datasourcetype","TEXT");
	private final JsonObject job = new JsonObject().put("id","SERIAL")
			.put("created_at","TIMESTAMP")
			.put("updated_at","TIMESTAMP")
			.put("data","JSONB")
			.put("status","INTEGER")
			.put("sourceid","BIGINT")
			.put("sourcetype","TEXT");
	private final JsonObject broker = new JsonObject().put("id","SERIAL")
			.put("created_at","TIMESTAMP")
			.put("updated_at","TIMESTAMP")
			.put("url","TEXT")
			.put("status","TEXT");
	private final JsonObject configuration = new JsonObject().put("id","SERIAL")
			.put("country","TEXT")
			.put("url","TEXT")
			.put("maintainer","TEXT")
			.put("curator","TEXT")
			.put("title","TEXT")
			.put("jwt","TEXT");

	public InitService(Vertx vertx){
		this.vertx = vertx;
	}

	public void initDatabase(Handler<AsyncResult<Void>> resultHandler){

		initTables(reply -> {
			if(reply.succeeded()){
				createAdminUser(reply2 -> {
					if (reply2.succeeded()) {
						resultHandler.handle(Future.succeededFuture());
					}
					else{
						LOGGER.error("Initialization failed.", reply2.cause());
						resultHandler.handle(Future.failedFuture(reply2.cause()));
					}
				});

			}
		});
	}

	private Future<List<JsonObject>> performUpdate(JsonObject query,String tablename){
		Promise<List<JsonObject>> queryPromise = Promise.promise();
		Future<List<JsonObject>> queryFuture = queryPromise.future();
		DatabaseConnector.getInstance().initTable(query,tablename, queryFuture);
		return queryFuture;
	}

	private Future<List<JsonObject>> performFileUploadTabelCreation(){
		Promise<List<JsonObject>> queryPromise = Promise.promise();
		Future<List<JsonObject>> queryFuture = queryPromise.future();
		DatabaseConnector.getInstance().query(DATASOURCEFILEUPLOAD_CREATE_QUERY, Tuple.tuple()
				.addString("File Upload")
				.addValue(new JsonObject())
				.addString("File Upload"), queryFuture);
		return queryFuture;
	}

	private void initTables(Handler<AsyncResult<Void>> resultHandler){

		ArrayList<Future> list = new ArrayList<Future>() {{
            performUpdate(user,"public.user");
            performUpdate(dataasset,"dataasset");
            performUpdate(datasource,"datasource");
            performUpdate(broker,"broker");
            performUpdate(job,"job");
            performUpdate(configuration,"configuration");
		}};

		CompositeFuture.all(list).onComplete( reply -> {
			if(reply.succeeded()) {
				Future fileUploadFuture = performFileUploadTabelCreation();
				fileUploadFuture.onComplete(reply2 -> {
					if(fileUploadFuture.succeeded()){
						LOGGER.info("Tables creation finished.");
						resultHandler.handle(Future.succeededFuture());
					} else {
						LOGGER.info("File Upload entry creation failed.");
						resultHandler.handle(Future.failedFuture(fileUploadFuture.cause()));
					}
				});
			}
			else{
				LOGGER.error("Tables creation failed", reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	private void createAdminUser(Handler<AsyncResult<Void>> resultHandler){

		ConfigStoreOptions confStore = new ConfigStoreOptions()
				.setType("env");

		ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(confStore);

		ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

		retriever.getConfig(ar -> {
			if (ar.succeeded()) {
				DatabaseConnector.getInstance().query(ADMIN_CREATE_QUERY, Tuple.tuple()
						.addString(ar.result().getJsonObject("FRONTEND_CONFIG").getString("username"))
						.addString(BCrypt.hashpw(ar.result().getJsonObject("FRONTEND_CONFIG").getString("password"), BCrypt.gensalt()))
						, reply -> {
					if (reply.succeeded()) {
						LOGGER.info("Adminuser created.");
						resultHandler.handle(Future.succeededFuture());
					} else {
						LOGGER.error("Adminuser creation failed.", reply.cause());
						resultHandler.handle(Future.failedFuture(reply.cause()));
					}
				});
			}
			else{
				LOGGER.error("ConfigRetriever failed.", ar.cause());
				resultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}
}
