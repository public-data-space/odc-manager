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
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DataAssetManager {

	private Logger LOGGER = LoggerFactory.getLogger(DataAssetManager.class.getName());
	private DatabaseService dbService;

	private static final String FINDBYID_QUERY = "SELECT * FROM DataAsset WHERE id = ?";
	private static final String FINDPUBLISHED_QUERY = "SELECT * FROM DataAsset WHERE status = ?";
	private static final String FINDALL_QUERY = "SELECT * FROM DataAsset ORDER BY id DESC";
	private static final String COUNT_QUERY = "SELECT COUNT(d) FROM DataAsset d";
	private static final String COUNTPUBLISHED_QUERY = "SELECT COUNT(d) FROM DataAsset d WHERE d.status = ?";
	private static final String CHANGESTATUS_UPDATE = "UPDATE DataAsset SET status = ?, updated_at = NOW() WHERE id = ?";
	private static final String ADDINITIAL_UPDATE = "INSERT INTO DataAsset (created_at, updated_at) values(?,?)";
	private static final String GETID_QUERY = "SELECT id FROM dataasset WHERE created_at = ? ";
	private static final String ADD_UPDATE = "Update DataAsset SET updated_at = NOW(), datasetid = ?, name = ?, url = ?,"
			+ " format = ?, licenseurl = ?, licensetitle = ?, datasettitle = ?, datasetnotes = ?, orignalresourceurl = ?,"
			+ " orignaldataseturl = ?, signature = ?, status = ?, resourceid = ?, tags = ?, datasetdescription = ?,"
			+ " organizationtitle = ?, organizationdescription = ?, version = ?, sourceid = ? WHERE id = ?";
	private static final String DELETE_UPDATE = "DELETE FROM dataasset WHERE id = ?";

	public DataAssetManager(Vertx vertx) {
		dbService = DatabaseService.createProxy(vertx, Constants.DATABASE_SERVICE);
	}

	public void findById(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
		dbService.query(FINDBYID_QUERY, new JsonArray(Arrays.asList(id)), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(reply.result().get(0)));
			}
		});
	}

	public void findPublished(Handler<AsyncResult<JsonArray>> resultHandler) {
		dbService.query(FINDPUBLISHED_QUERY, new JsonArray(Arrays.asList(DataAssetStatus.PUBLISHED.ordinal())), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(new JsonArray(reply.result())));
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

	public void count(Handler<AsyncResult<Long>> resultHandler) {
		dbService.query(COUNT_QUERY, new JsonArray(), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(reply.result().get(0).getLong("count")));
			}
		});
	}

	public void countPublished(Handler<AsyncResult<Long>> resultHandler) {
		dbService.query(COUNTPUBLISHED_QUERY,new JsonArray(Arrays.asList(DataAssetStatus.PUBLISHED.ordinal())), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(reply.result().get(0).getLong("count")));
			}
		});
	}

	public void changeStatus(DataAssetStatus status, Long id, Handler<AsyncResult<Void>> resultHandler) {
		dbService.update(CHANGESTATUS_UPDATE,new JsonArray().add(status.ordinal()).add(id), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}

	public void addInitial(Handler<AsyncResult<Long>> resultHandler){
		Date d = new Date();
		JsonArray params = new JsonArray()
				.add(d.toInstant())
				.add(d.toInstant());

		dbService.update(ADDINITIAL_UPDATE, params, reply -> {
			if (reply.succeeded()) {
				dbService.query(GETID_QUERY, new JsonArray().add(d.toInstant()), reply2 -> {
					if(reply2.succeeded()){
						if(reply2.result().size() == 1) {
							resultHandler.handle(Future.succeededFuture(reply2.result().get(0).getLong("id")));
						}
						else{
							LOGGER.error("Concurrency exception.");
							resultHandler.handle(Future.failedFuture("Concurrency exception."));
						}
					}
					else{
						LOGGER.error(reply2.cause());
						resultHandler.handle(Future.failedFuture(reply2.cause()));
					}
				});
			} else {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	public void add(JsonObject dataAssetJson, Handler<AsyncResult<Void>> resultHandler) {

		DataAsset dataAsset = Json.decodeValue(dataAssetJson.toString(),DataAsset.class);

		JsonArray params = new JsonArray()
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
				.add(checkNull(dataAsset.getSourceID().toString()))
				.add(dataAsset.getId());

		dbService.update(ADD_UPDATE,params, reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}

	public void delete(Long id, Handler<AsyncResult<Void>> resultHandler) {
		dbService.update(DELETE_UPDATE, new JsonArray().add(id), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}
}
