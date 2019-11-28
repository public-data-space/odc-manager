package de.fraunhofer.fokus.ids.controllers;

import de.fraunhofer.fokus.ids.messages.DataAssetCreateMessage;
import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.models.DataAssetDescription;
import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.entities.Job;
import de.fraunhofer.fokus.ids.persistence.enums.DataAssetStatus;
import de.fraunhofer.fokus.ids.persistence.enums.JobStatus;
import de.fraunhofer.fokus.ids.persistence.managers.DataAssetManager;
import de.fraunhofer.fokus.ids.persistence.managers.DataSourceManager;
import de.fraunhofer.fokus.ids.persistence.managers.JobManager;
import de.fraunhofer.fokus.ids.services.datasourceAdapter.DataSourceAdapterService;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DataAssetController {

	private Logger LOGGER = LoggerFactory.getLogger(DataAssetController.class.getName());
	private DataAssetManager dataAssetManager;
	private DataSourceAdapterService dataSourceAdapterService;
	private DataSourceManager dataSourceManager;
	private JobManager jobManager;
	private BrokerController brokerController;

	public DataAssetController(Vertx vertx) {
		dataAssetManager = new DataAssetManager(vertx);
		jobManager = new JobManager(vertx);
		this.dataSourceManager = new DataSourceManager(vertx);
		dataSourceAdapterService = DataSourceAdapterService.createProxy(vertx, Constants.DATASOURCEADAPTER_SERVICE);
		brokerController = new BrokerController(vertx);
	}

	public void counts(Handler<AsyncResult<JsonObject>> resultHandler) {

		Future<Long> count = Future.future();
		dataAssetManager.count( reply -> {
			if (reply.succeeded()) {
				count.complete(reply.result());
			}
			else {
				LOGGER.error("Count could not be queried.\n\n"+reply.cause());
				count.fail(reply.cause());
			}
		});

		Future<Long> countPublished = Future.future();
		dataAssetManager.countPublished(reply -> {
			if (reply.succeeded()) {
				countPublished.complete(reply.result());
			}
			else {
				LOGGER.error("Published count could not be queried.\n\n"+reply.cause());
				countPublished.fail(reply.cause());
			}
		});

		CompositeFuture.all(count, countPublished).setHandler(ar -> {
			if(ar.succeeded()) {
				JsonObject jO = new JsonObject();
				jO.put("dacount", count.result());
				jO.put("publishedcount", countPublished.result());
				resultHandler.handle(Future.succeededFuture(jO));
			}
			else {
				LOGGER.error("Composite Future failed.\n\n"+ar.cause());
				resultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

	public void add(DataAssetDescription dataAssetDescription, Handler<AsyncResult<JsonObject>> resultHandler) {

		if (dataAssetDescription.getData().isEmpty()) {
			JsonObject jO = new JsonObject();
			jO.put("status", "error");
			jO.put("text", "Bitte geben Sie eine Resource-ID ein!");
			resultHandler.handle(Future.succeededFuture(jO));
		} else {
			jobManager.add(new JsonObject(Json.encode(dataAssetDescription)), jobReply -> {
				if (jobReply.succeeded()) {
					long jobId = jobReply.result().getLong("id");
					LOGGER.info("Starting Job with ID: " + jobId);
					jobManager.updateStatus(jobId, JobStatus.RUNNING, statusUpdateReply -> {});
					initiateDataAssetCreation(da -> createDataAsset(jobId, da), dataAssetDescription);
					JsonObject jO = new JsonObject();
					jO.put("status", "success");
					jO.put("text", "Job wurde erstellt!");
					resultHandler.handle(Future.succeededFuture(jO));
				} else {
					LOGGER.error("Der Job konnte nicht erstellt werden!", jobReply.cause());
					JsonObject jO = new JsonObject();
					jO.put("status", "error");
					jO.put("text", "Der Job konnte nicht erstellt werden!");
					resultHandler.handle(Future.succeededFuture(jO));
				}
			});
		}
	}

	private void initiateDataAssetCreation(Handler<AsyncResult<DataAsset>> next, DataAssetDescription dataAssetDescription) {
			dataAssetManager.addInitial(initCreateReply -> {
				if(initCreateReply.succeeded()){
					dataSourceManager.findById(Integer.toUnsignedLong(dataAssetDescription.getSourceId()), dataSourceReply -> {
						if (dataSourceReply.succeeded()) {
							DataSource dataSource = Json.decodeValue(dataSourceReply.result().toString(), DataSource.class);

							DataAssetCreateMessage mes = new DataAssetCreateMessage();
							mes.setData(new JsonObject(dataAssetDescription.getData()));
							mes.setDataSource(dataSource);
							mes.setDataAssetId(initCreateReply.result());

							dataSourceAdapterService.createDataAsset(dataSource.getDatasourceType(), new JsonObject(Json.encode(mes)), dataAssetCreateReply-> {
								if (dataAssetCreateReply.succeeded()) {
									if (dataAssetCreateReply.result()==null){
										dataAssetManager.delete(initCreateReply.result(), deleteReply -> {
											if(deleteReply.succeeded()){
												LOGGER.error(dataAssetCreateReply.cause());
												next.handle(Future.failedFuture(dataAssetCreateReply.cause()));
											} else{
												LOGGER.info("INCONSISTENCY IN DATABASE. There is a DataAsset object in the database with no corresponding object in the adapter.");
												next.handle(Future.failedFuture(deleteReply.cause()));
											}
										});
									}
									else {
										next.handle(Future.succeededFuture(Json.decodeValue(dataAssetCreateReply.result().toString(), DataAsset.class)));
									}
								} else {
									LOGGER.error(dataAssetCreateReply.cause());
									next.handle(Future.failedFuture(dataAssetCreateReply.cause()));
								}
							});
						} else {
							LOGGER.error(dataSourceReply.cause());
							next.handle(Future.failedFuture(dataSourceReply.cause()));
						}
					});
				}
				else{
					LOGGER.error(initCreateReply.cause());
					next.handle(Future.failedFuture(initCreateReply.cause()));
				}
			});
		}

	private void createDataAsset(long jobId, AsyncResult<DataAsset> res) {
		if (res.succeeded()) {
			LOGGER.info("DataAsset was successfully created.");

			dataAssetManager.add(new JsonObject(Json.encode(res.result())), reply -> {
				if (reply.succeeded()) {
					LOGGER.info("DataAsset was successfully inserted to the DB.");
					jobManager.updateStatus(jobId, JobStatus.FINISHED, reply2 -> {});
				} else {
					LOGGER.error("DataAsset insertion failed.", res.cause());
					jobManager.updateStatus(jobId, JobStatus.ERROR, reply2 -> {});
				}
			});
		} else {
			LOGGER.error("DataAsset Creation failed.", res.cause());
			jobManager.updateStatus(jobId, JobStatus.ERROR, reply2 -> {});
		}
	}


	public void publish(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
		dataAssetManager.changeStatus(DataAssetStatus.PUBLISHED, id, reply -> {
			JsonObject jO = new JsonObject();
			if (reply.succeeded()) {
				jO.put("success", "Data Asset " + id + " wurde veröffentlicht.");
				resultHandler.handle(Future.succeededFuture(jO));
				brokerController.update();
			}
			else {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	public void unPublish(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
		dataAssetManager.changeStatus(DataAssetStatus.APPROVED, id, reply -> {
			JsonObject jO = new JsonObject();
			if (reply.succeeded()) {
				jO.put("success", "Data Asset " + id + " wurde zurückgehalten.");
				resultHandler.handle(Future.succeededFuture(jO));
				brokerController.update();
			}
			else {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));


			}
		});
	}

	public void delete(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
		dataAssetManager.findById(id, dataAssetReply -> {
			if(dataAssetReply.succeeded()){
				dataSourceManager.findById(Json.decodeValue(dataAssetReply.result().toString(), DataAsset.class).getSourceID(), reply2 -> {
					if(reply2.succeeded()){
						Future serviceDeleteFuture = Future.future();
						dataSourceAdapterService.delete(reply2.result().getString("datasourcetype"), id, serviceDeleteFuture.completer());

						Future databaseDeleteFuture = Future.future();
						dataAssetManager.delete(id, databaseDeleteFuture.completer());

						CompositeFuture.all(databaseDeleteFuture, serviceDeleteFuture).setHandler( ar -> {
							if(ar.succeeded()){
								JsonObject jO = new JsonObject();
								jO.put("status", "success");
								jO.put("text", "Data Asset " + id + " wurde gelöscht.");
								resultHandler.handle(Future.succeededFuture(jO));
								brokerController.update();
							} else {
								LOGGER.error("Delete Future could not be completed.", ar.cause());
								resultHandler.handle(Future.failedFuture(ar.cause()));
							}
						});

					} else {
						resultHandler.handle(Future.failedFuture(reply2.cause()));
					}
				});
			} else {
				resultHandler.handle(Future.failedFuture(dataAssetReply.cause()));
			}
		});
	}

	public void index(Handler<AsyncResult<JsonArray>> resultHandler) {
		dataAssetManager.findAll(reply -> {
			if (reply.succeeded()) {
				resultHandler.handle(Future.succeededFuture(reply.result()));

			}
			else {
				LOGGER.error("FindAll Future could not be completed.\n\n" + reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	public void resource(Message<Object> receivedMessage) {
		//TODO Get REsource from Adapter
	}

}
