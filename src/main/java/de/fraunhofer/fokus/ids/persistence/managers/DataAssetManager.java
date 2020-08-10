package de.fraunhofer.fokus.ids.persistence.managers;

import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import de.fraunhofer.fokus.ids.persistence.enums.DataAssetStatus;
import de.fraunhofer.fokus.ids.persistence.util.DatabaseConnector;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.Tuple;

import static de.fraunhofer.fokus.ids.persistence.util.Functions.checkNull;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DataAssetManager {

	private Logger LOGGER = LoggerFactory.getLogger(DataAssetManager.class.getName());
	private DatabaseConnector databaseConnector;

	private static final String FINDBYID_QUERY = "SELECT * FROM DataAsset WHERE id = $1";
	private static final String FINDPUBLISHED_QUERY = "SELECT * FROM DataAsset WHERE status = $1";
	private static final String FINDALL_QUERY = "SELECT * FROM DataAsset ORDER BY id DESC";
	private static final String COUNT_QUERY = "SELECT COUNT(d) FROM DataAsset d";
	private static final String COUNTPUBLISHED_QUERY = "SELECT COUNT(d) FROM DataAsset d WHERE d.status = $1";
	private static final String CHANGESTATUS_UPDATE = "UPDATE DataAsset SET status = $1, updated_at = NOW() WHERE id = $2";
	private static final String ADDINITIAL_UPDATE = "INSERT INTO DataAsset (created_at, updated_at) values(NOW(),NOW()) RETURNING id";
	private static final String ADD_UPDATE = "Update DataAsset SET updated_at = NOW(), datasetid = $1, name = $2, url = $3,"
			+ " format = $4, licenseurl = $5, licensetitle = $6, datasettitle = $7, datasetnotes = $8, orignalresourceurl = $9,"
			+ " orignaldataseturl = $10, signature = $11, status = $12, resourceid = $13, tags = $14, datasetdescription = $15,"
			+ " organizationtitle = $16, organizationdescription = $17, version = $18, sourceid = $19, filename = $20 WHERE id = $21";
	private static final String DELETE_UPDATE = "DELETE FROM dataasset WHERE id = $1";

	public DataAssetManager() {
		databaseConnector = DatabaseConnector.getInstance();
	}

	public void findById(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
		databaseConnector.query(FINDBYID_QUERY, Tuple.tuple().addLong(id),reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			} else {
				if(reply.result().isEmpty()){
					resultHandler.handle(Future.failedFuture("DataAsset id not in database"));
				} else {
					resultHandler.handle(Future.succeededFuture(reply.result().get(0)));
				}
			}
		});
	}

	public void findPublished(Handler<AsyncResult<JsonArray>> resultHandler) {
		databaseConnector.query(FINDPUBLISHED_QUERY, Tuple.tuple().addInteger(DataAssetStatus.PUBLISHED.ordinal()), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(new JsonArray(reply.result())));
			}
		});
	}

	public void findAll(Handler<AsyncResult<JsonArray>> resultHandler) {
		databaseConnector.query(FINDALL_QUERY, Tuple.tuple(), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(new JsonArray(reply.result())));
			}
		});
	}

	public void count(Handler<AsyncResult<Long>> resultHandler) {
		databaseConnector.query(COUNT_QUERY, Tuple.tuple(), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(reply.result().get(0).getLong("count")));
			}
		});
	}

	public void countPublished(Handler<AsyncResult<Long>> resultHandler) {
		databaseConnector.query(COUNTPUBLISHED_QUERY,Tuple.tuple().addInteger(DataAssetStatus.PUBLISHED.ordinal()), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(reply.result().get(0).getLong("count")));
			}
		});
	}

	public void changeStatus(DataAssetStatus status, Long id, Handler<AsyncResult<Void>> resultHandler) {
		databaseConnector.query(CHANGESTATUS_UPDATE, Tuple.tuple().addInteger(status.ordinal()).addLong(id), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}

	public void addInitial(Handler<AsyncResult<Long>> resultHandler){
		databaseConnector.query(ADDINITIAL_UPDATE, Tuple.tuple(), reply -> {
			if (reply.succeeded()) {
				resultHandler.handle(Future.succeededFuture(reply.result().get(0).getLong("id")));
			} else {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	public void add(JsonObject dataAssetJson, Handler<AsyncResult<Void>> resultHandler) {

		DataAsset dataAsset = Json.decodeValue(dataAssetJson.toString(),DataAsset.class);

		Tuple params = Tuple.tuple()
				.addString(checkNull(dataAsset.getDatasetID()))
				.addString(checkNull(dataAsset.getName()))
				.addString(checkNull(dataAsset.getUrl()))
				.addString(checkNull(dataAsset.getFormat()))
				.addString(checkNull(dataAsset.getLicenseUrl()))
				.addString(checkNull(dataAsset.getLicenseTitle()))
				.addString(checkNull(dataAsset.getDatasetTitle()))
				.addString(checkNull(dataAsset.getDatasetNotes()))
				.addString(checkNull(dataAsset.getOrignalResourceURL()))
				.addString(checkNull(dataAsset.getOrignalDatasetURL()))
				.addString(checkNull(dataAsset.getSignature()))
				.addInteger(dataAsset.getStatus() == null ? DataAssetStatus.UNAPPROVED.ordinal() : dataAsset.getStatus().ordinal())
				.addString(checkNull(dataAsset.getResourceID()))
				.addStringArray(dataAsset.getTags() == null ||dataAsset.getTags().isEmpty() ? new String[0] : dataAsset.getTags().toArray(new String[0]))
				.addString(checkNull(dataAsset.getDataSetDescription()))
				.addString(checkNull(dataAsset.getOrganizationTitle()))
				.addString(checkNull(dataAsset.getOrganizationDescription()))
				.addString(checkNull(dataAsset.getVersion()))
				.addString(checkNull(dataAsset.getSourceID().toString()))
                .addString(checkNull(dataAsset.getFilename()))
                .addLong(dataAsset.getId());

		databaseConnector.query(ADD_UPDATE,params, reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}

	public void delete(Long id, Handler<AsyncResult<Void>> resultHandler) {
		databaseConnector.query(DELETE_UPDATE, Tuple.tuple().addLong(id), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}
}
