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

import java.util.HashMap;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class JobManager {

	private DatabaseService dbService;
	private Logger LOGGER = LoggerFactory.getLogger(JobManager.class.getName());

	private static final String ADD_QUERY = "INSERT INTO job (created_at,updated_at,data,status,sourceid, sourcetype) values (NOW(), NOW(), ?, ?, ?, ?)";
	private static final String FINDALL_QUERY = "SELECT * FROM job";
	private static final String DELETEALL_QUERY = "DELETE FROM job";
	private static final String FINDUNFINISHED_QUERY = "SELECT * FROM job WHERE status=?";
	private static final String UPDATESTATUS_QUERY = "UPDATE job SET status = ?, updated_at = NOW() WHERE id = ?";

	public JobManager(Vertx vertx) {
		dbService = DatabaseService.createProxy(vertx, Constants.DATABASE_SERVICE);
	}

	public void add(JsonObject dataAssetDescriptionJson, Handler<AsyncResult<JsonObject>> resultHandler) {
		DataAssetDescription dataAssetDescription = Json.decodeValue(dataAssetDescriptionJson.toString(), DataAssetDescription.class);

		JsonArray params = new JsonArray()
				.add(new JsonObject((dataAssetDescription.getData().isEmpty() ? new HashMap<>() : dataAssetDescription.getData())).toString())
				.add(JobStatus.CREATED.ordinal())
				.add(dataAssetDescription.getSourceId())
				.add(dataAssetDescription.getDatasourcetype());

		dbService.update(ADD_QUERY, params, reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}

	public void findAll(Handler<AsyncResult<JsonArray>> resultHandler) {
		dbService.query(FINDALL_QUERY, new JsonArray(), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(new JsonArray(reply.result())));
			}
		});
	}

	public void deleteAll(Handler<AsyncResult<Void>> resultHandler) {
		dbService.update(DELETEALL_QUERY, new JsonArray(), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}

	public void findUnfinished(Handler<AsyncResult<JsonObject>> resultHandler) {
		dbService.query(FINDUNFINISHED_QUERY, new JsonArray().add(0), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(reply.result().get(0)));
			}
		});
	}

	public void updateStatus(Long id, JobStatus status, Handler<AsyncResult<Void>> resultHandler) {
		dbService.query(UPDATESTATUS_QUERY, new JsonArray().add(status.ordinal()).add(id), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}

}
