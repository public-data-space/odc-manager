package de.fraunhofer.fokus.ids.services;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class InitService extends AbstractVerticle{

	final Logger LOGGER = LoggerFactory.getLogger(InitService.class.getName());

	private EventBus eb;
	private JsonObject config;
	private String ROUTE_PREFIX = "de.fraunhofer.fokus.ids.";

	@Override
	public void start(Future<Void> startFuture) {
		this.eb = vertx.eventBus();
		Future<JsonObject> configFuture = getConfigFuture();
		configFuture.setHandler(ac -> {
			if(ac.succeeded()) {
				this.config = ac.result();
				initDefaultResources();
			}
			else {
				LOGGER.error("Config Future could not be completed.");
			}
		});
		initDefaultResources();
	}

	private void initDefaultResources() {
		if(config.getBoolean("init.enabled")) {
			JsonArray initResources = config.getJsonArray("init.resources");
			initResources.stream().forEach(resourceID -> {
				eb.send(ROUTE_PREFIX+"dataAssetManager.resourceIDexists", resourceID, res -> {
					if(res.succeeded()) {
						if((Boolean)res.result().body()) {
							eb.send(ROUTE_PREFIX+"jobManager.add", resourceID, job -> {
								if(job.succeeded()) {
									eb.send(ROUTE_PREFIX+"jobService.process", job.result().body().toString());
								}
								else {
									LOGGER.error("Job could not be added.");
								} 
							});
						}
						else {
							LOGGER.error("ResourceId does not exist.");
						}
					}
					else {
						LOGGER.error("ResourceId's existance could not be checked.");
					}
				});
			});
		}
	}

	private Future<JsonObject> getConfigFuture() {
		Future<JsonObject> configFuture = Future.future();
		eb.send(ROUTE_PREFIX+"repositoryService.configRetrieverVerticle.getConfig", "", res -> {
			if(res.succeeded()) {
				configFuture.complete(Json.decodeValue(res.result().body().toString(),JsonObject.class));
			}
			else {
				LOGGER.error("Config could not be loaded.");
				configFuture.fail("Config could not be loaded.");
			}
		});
		return configFuture;
	}
}
