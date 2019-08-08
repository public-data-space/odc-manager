package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.persistence.service.DatabaseService;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.mindrot.jbcrypt.BCrypt;


public class InitService{

	final Logger LOGGER = LoggerFactory.getLogger(InitService.class.getName());

	private DatabaseService databaseService;
	private Vertx vertx;

	public InitService(Vertx vertx){
		this.databaseService = DatabaseService.createProxy(vertx, Constants.DATABASE_SERVICE);
		this.vertx = vertx;
	}

	public void initDatabase(Handler<AsyncResult<Void>> resultHandler){

		createAdminUser(reply -> {
			if (reply.succeeded()) {
				resultHandler.handle(Future.succeededFuture());
			}
			else{
				LOGGER.info("Initialization failed.", reply.cause());
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
				databaseService.update("INSERT INTO public.user(created_at, updated_at, username, password) \n" +
						"    SELECT NOW(), NOW(), ?, ?\n" +
						"WHERE NOT EXISTS (\n" +
						"    SELECT 1 FROM public.user WHERE username=?\n" +
						");", new JsonArray()
						.add(ar.result().getString("FRONTEND_ADMIN"))
						.add(BCrypt.hashpw(ar.result().getString("FRONTEND_ADMIN_PW"), BCrypt.gensalt()))
						.add(ar.result().getString("FRONTEND_ADMIN")), reply -> {
					if (reply.succeeded()) {
						resultHandler.handle(Future.succeededFuture());
					} else {
						LOGGER.info("Adminuser creation failed.", reply.cause());
						resultHandler.handle(Future.failedFuture(reply.cause()));
					}
				});
			}
			else{

			}
		});
	}

	private void initDefaultResources() {
	//TODO
	}
}
