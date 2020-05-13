package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.persistence.service.DatabaseService;
import edu.emory.mathcs.backport.java.util.Arrays;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.mindrot.jbcrypt.BCrypt;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class InitService{

	private final Logger LOGGER = LoggerFactory.getLogger(InitService.class.getName());

	private DatabaseService databaseService;
	private Vertx vertx;

	private final String ADMIN_CREATE_QUERY = "INSERT INTO public.user(created_at, updated_at, username, password) SELECT NOW(), NOW(), ?, ? WHERE NOT EXISTS ( SELECT 1 FROM public.user WHERE username=?)";
	private final String DATASOURCEFILEUPLOAD_CREATE_QUERY = "INSERT INTO datasource(created_at, updated_at, datasourcename,data, datasourcetype) SELECT NOW(), NOW(), ?, ? ,? WHERE NOT EXISTS ( SELECT 1 FROM datasource WHERE datasourcetype=?)";
	private final String USER_TABLE_CREATE_QUERY = "CREATE TABLE IF NOT EXISTS public.user (id SERIAL , created_at TIMESTAMP , updated_at TIMESTAMP , username TEXT, password TEXT)";
	private final String DATAASSET_TABLE_CREATE_QUERY = "CREATE TABLE IF NOT EXISTS dataasset (id SERIAL, created_at TIMESTAMP, updated_at TIMESTAMP, datasetid TEXT, name TEXT, url TEXT, format TEXT, licenseurl TEXT, licensetitle TEXT, datasettitle TEXT, datasetnotes TEXT, orignalresourceurl TEXT, orignaldataseturl TEXT, signature TEXT, status INTEGER, resourceid TEXT, tags TEXT[] , datasetdescription TEXT, organizationtitle TEXT, organizationdescription TEXT, version TEXT, sourceid TEXT)";
	private final String DATASOURCE_TABLE_CREATE_QUERY = "CREATE TABLE IF NOT EXISTS datasource (id SERIAL, created_at TIMESTAMP, updated_at TIMESTAMP, datasourcename TEXT, data JSONB, datasourcetype TEXT)";
	private final String JOB_TABLE_CREATE_QUERY = "CREATE TABLE IF NOT EXISTS job (id SERIAL, created_at TIMESTAMP, updated_at TIMESTAMP, data JSONB, status INTEGER, sourceid BIGINT, sourcetype TEXT)";
	private final String BROKER_TABLE_CREATE_QUERY = "CREATE TABLE IF NOT EXISTS broker (id SERIAL, created_at TIMESTAMP, updated_at TIMESTAMP, url TEXT, status TEXT)";
	private final String CONFIGURATION_TABLE_CREATE_QUERY = "CREATE TABLE IF NOT EXISTS configuration (id SERIAL, country TEXT, url TEXT, maintainer TEXT, curator TEXT, title TEXT)";

	public InitService(Vertx vertx){
		this.databaseService = DatabaseService.createProxy(vertx, Constants.DATABASE_SERVICE);
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

	private Future<List<JsonObject>> performUpdate(String query){
		Future<List<JsonObject>> queryFuture = Future.future();
		databaseService.update(query, new JsonArray(), queryFuture.completer());
		return queryFuture;
	}

	private void initTables(Handler<AsyncResult<Void>> resultHandler){
		CompositeFuture.all(performUpdate(USER_TABLE_CREATE_QUERY),
				performUpdate(DATAASSET_TABLE_CREATE_QUERY),
				performUpdate(DATASOURCE_TABLE_CREATE_QUERY),
				performUpdate(BROKER_TABLE_CREATE_QUERY),
				performUpdate(JOB_TABLE_CREATE_QUERY),
				performUpdate(CONFIGURATION_TABLE_CREATE_QUERY)).setHandler( reply -> {
			if(reply.succeeded()) {
				LOGGER.info("Tables creation finished.");
				resultHandler.handle(Future.succeededFuture());
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
				databaseService.update(ADMIN_CREATE_QUERY, new JsonArray()
						.add(ar.result().getString("FRONTEND_ADMIN"))
						.add(BCrypt.hashpw(ar.result().getString("FRONTEND_ADMIN_PW"), BCrypt.gensalt()))
						.add(ar.result().getString("FRONTEND_ADMIN")), reply -> {
					if (reply.succeeded()) {
						LOGGER.info("Adminuser created.");
						JsonObject jsonObject = new JsonObject();
						databaseService.update(DATASOURCEFILEUPLOAD_CREATE_QUERY, new JsonArray()
								.add("File Upload")
								.add(jsonObject)
								.add("File Upload")
								.add("File Upload"), reply2 -> {
							if (reply2.succeeded()) {
								LOGGER.info("FileUpload DataSource created.");
								resultHandler.handle(Future.succeededFuture());
							} else {
								LOGGER.error("FileUpload DataSource creation failed.", reply2.cause());
								resultHandler.handle(Future.failedFuture(reply2.cause()));
							}
						});
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
