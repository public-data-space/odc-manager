package de.fraunhofer.fokus.ids.persistence.managers;

import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.models.DataAssetDescription;
import de.fraunhofer.fokus.ids.persistence.enums.JobStatus;
import de.fraunhofer.fokus.ids.persistence.service.DatabaseService;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.Date;
import java.util.HashMap;

public class JobManager {

	DatabaseService dbService;
	private Logger LOGGER = LoggerFactory.getLogger(JobManager.class.getName());

	public JobManager(Vertx vertx) {
		dbService = DatabaseService.createProxy(vertx, Constants.DATABASE_SERVICE);
	}

	public void add(JsonObject dataAssetDescriptionJson, Handler<AsyncResult<JsonObject>> resultHandler) {
		DataAssetDescription dataAssetDescription = Json.decodeValue(dataAssetDescriptionJson.toString(), DataAssetDescription.class);

		String update = "INSERT INTO job (created_at,updated_at,data,status,sourceid, sourcetype) values ( ?, ?, ?, ?, ?, ?)";
		Date d = new Date();
		JsonArray params = new JsonArray()
				.add(d.toInstant())
				.add(d.toInstant())
				.add(new JsonObject((dataAssetDescription.getData().isEmpty() ? new HashMap<>() : dataAssetDescription.getData())).toString())
				.add(JobStatus.CREATED.ordinal())
				.add(dataAssetDescription.getSourceId())
				.add(dataAssetDescription.getDatasourcetype());

		dbService.update(update, params, reply -> {
			if (reply.failed()) {
				LOGGER.info(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});

	}

	public void findAll(Handler<AsyncResult<JsonArray>> resultHandler) {
		dbService.query("SELECT * FROM job", new JsonArray(), reply -> {
			if (reply.failed()) {
				LOGGER.info(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(new JsonArray(reply.result())));
			}
		});
	}

	public void deleteAll(Handler<AsyncResult<Void>> resultHandler) {
		dbService.update("DELETE FROM job", new JsonArray(), reply -> {
			if (reply.failed()) {
				LOGGER.info(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}

	public void findUnfinished(Handler<AsyncResult<JsonObject>> resultHandler) {
		dbService.query("SELECT * FROM job WHERE status=?", new JsonArray().add(0), reply -> {
			if (reply.failed()) {
				LOGGER.info(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(reply.result().get(0)));
			}
		});
	}

	public void updateStatus(Long id, JobStatus status, Handler<AsyncResult<Void>> resultHandler) {
		Date d = new Date();
		dbService.query("UPDATE job SET status = ?, updated_at = ? WHERE id = ?", new JsonArray().add(status.ordinal()).add(d.toInstant()).add(id), reply -> {
			if (reply.failed()) {
				LOGGER.info(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}

}
