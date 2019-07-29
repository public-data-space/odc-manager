package de.fraunhofer.fokus.ids.persistence.managers;

import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import de.fraunhofer.fokus.ids.persistence.enums.DataAssetStatus;
import de.fraunhofer.fokus.ids.persistence.service.DatabaseService;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import static de.fraunhofer.fokus.ids.persistence.util.Functions.checkNull;

public class DataAssetManager {

	private Logger LOGGER = LoggerFactory.getLogger(DataAssetManager.class.getName());

	DatabaseService dbService;


	public DataAssetManager(Vertx vertx) {
		dbService = DatabaseService.createProxy(vertx, Constants.DATABASE_SERVICE);
	}

	public void findById(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
		dbService.query("SELECT * FROM DataAsset WHERE id = ?", new JsonArray(Arrays.asList(id)), reply -> {
			if (reply.failed()) {
				LOGGER.info(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(reply.result().get(0)));
			}
		});
	}

	public void findPublished(Handler<AsyncResult<JsonArray>> resultHandler) {
		dbService.query("SELECT * FROM DataAsset WHERE status = ?",new JsonArray(Arrays.asList(DataAssetStatus.PUBLISHED.ordinal())), reply -> {
			if (reply.failed()) {
				LOGGER.info(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(new JsonArray(reply.result())));
			}
		});
	}

	public void findAll(Handler<AsyncResult<JsonArray>> resultHandler) {
		dbService.query("SELECT * FROM DataAsset ORDER BY id DESC", new JsonArray(), reply -> {
			if (reply.failed()) {
				LOGGER.info(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(new JsonArray(reply.result())));
			}
		});
	}

	public void count(Handler<AsyncResult<Long>> resultHandler) {
		dbService.query("SELECT COUNT(d) FROM DataAsset d", new JsonArray(), reply -> {
			if (reply.failed()) {
				LOGGER.info(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(reply.result().get(0).getLong("count")));
			}
		});
	}

	public void countPublished(Handler<AsyncResult<Long>> resultHandler) {
		dbService.query("SELECT COUNT(d) FROM DataAsset d WHERE d.status = ?",new JsonArray(Arrays.asList(DataAssetStatus.PUBLISHED.ordinal())), reply -> {
			if (reply.failed()) {
				LOGGER.info(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(reply.result().get(0).getLong("count")));
			}
		});
	}

	public void resourceIdExists(Long id, Handler<AsyncResult<Boolean>> resultHandler) {

	}

	public void changeStatus(DataAssetStatus status, Long id, Handler<AsyncResult<Void>> resultHandler) {
		Date d = new Date();
		dbService.update("UPDATE DataAsset SET status = ?, updated_at = ? WHERE id = ?",new JsonArray().add(status.ordinal()).add(d.toInstant()).add(id), reply -> {
			if (reply.failed()) {
				LOGGER.info(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}

	public void add(JsonObject dataAssetJson, Handler<AsyncResult<Void>> resultHandler) {

		DataAsset dataAsset = Json.decodeValue(dataAssetJson.toString(),DataAsset.class);

		String update = "INSERT INTO DataAsset (created_at, updated_at, datasetid, name, url, format, licenseurl, "
				+ "licensetitle, datasettitle, datasetnotes, orignalresourceurl, orignaldataseturl, "
				+ "signature, status, resourceid, tags, datasetdescription, organizationtitle, "
				+ "organizationdescription, version, sourceid) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		Date d = new Date();
		JsonArray params = new JsonArray()
				.add(d.toInstant())
				.add(d.toInstant())
				.add(checkNull(dataAsset.getDatasetID()))
				.add(checkNull(dataAsset.getName()))
				.add(checkNull(dataAsset.getUrl()))
				.add(checkNull(dataAsset.getFormat()))
				.add(checkNull(dataAsset.getLicenseUrl()))
				.add(checkNull(dataAsset.getLicenseTitle()))
				.add(checkNull(dataAsset.getDatasetTitle()))
				.add(checkNull(dataAsset.getDatasetNotes()))
				.add(checkNull(dataAsset.getOrignalResourceURL()))
				.add(checkNull(dataAsset.getOrignalDatasetURL()))
				.add(checkNull(dataAsset.getSignature()))
				.add(dataAsset.getStatus() == null ? DataAssetStatus.UNAPPROVED.ordinal() : dataAsset.getStatus().ordinal())
				.add(checkNull(dataAsset.getResourceID()))
				.add(dataAsset.getTags() == null ||dataAsset.getTags().isEmpty() ? new ArrayList<String>(): dataAsset.getTags())
				.add(checkNull(dataAsset.getDataSetDescription()))
				.add(checkNull(dataAsset.getOrganizationTitle()))
				.add(checkNull(dataAsset.getOrganizationDescription()))
				.add(checkNull(dataAsset.getVersion()))
				.add(checkNull(dataAsset.getSourceID()));

		dbService.update(update,params, reply -> {
			if (reply.failed()) {
				LOGGER.info(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}

	public void delete(Long id, Handler<AsyncResult<Void>> resultHandler) {
		dbService.update("DELETE FROM dataasset WHERE id = ?",new JsonArray().add(id), reply -> {
			if (reply.failed()) {
				LOGGER.info(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}
}
