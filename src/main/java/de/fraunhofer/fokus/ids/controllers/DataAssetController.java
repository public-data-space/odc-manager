package de.fraunhofer.fokus.ids.controllers;

import de.fraunhofer.fokus.ids.messages.DataAssetCreateMessage;
import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.models.DataAssetDescription;
import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
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

import java.util.ArrayList;

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
        dataAssetManager.count(reply -> {
            if (reply.succeeded()) {
                count.complete(reply.result());
            } else {
                LOGGER.error("Count could not be queried.\n\n" + reply.cause());
                count.fail(reply.cause());
            }
        });

        Future<Long> countPublished = Future.future();
        dataAssetManager.countPublished(reply -> {
            if (reply.succeeded()) {
                countPublished.complete(reply.result());
            } else {
                LOGGER.error("Published count could not be queried.\n\n" + reply.cause());
                countPublished.fail(reply.cause());
            }
        });

        CompositeFuture.all(count, countPublished).setHandler(ar -> {
            if (ar.succeeded()) {
                JsonObject jO = new JsonObject();
                jO.put("dacount", count.result());
                jO.put("publishedcount", countPublished.result());
                resultHandler.handle(Future.succeededFuture(jO));
            } else {
                LOGGER.error("Composite Future failed.\n\n" + ar.cause());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void add(DataAssetDescription dataAssetDescription, String licenceurl, String licencetitle, Handler<AsyncResult<JsonObject>> resultHandler) {
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
                    jobManager.updateStatus(jobId, JobStatus.RUNNING, statusUpdateReply -> {
                    });
                    initiateDataAssetCreation(da -> createDataAsset(jobId, da, licenceurl, licencetitle), dataAssetDescription);
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
            if (initCreateReply.succeeded()) {
                if (dataAssetDescription.getDatasourcetype().equals("File Upload")){
                    dataSourceManager.findByType("File Upload",dataSourceReply -> {
                        DataAsset dataAsset = new DataAsset();
                        if (dataSourceReply.succeeded()){
                            String dts = dataSourceReply.result().toString().replace("[","").replace("]","");
                            DataSource dataSource = Json.decodeValue(dts, DataSource.class);
                            DataAssetCreateMessage mes = new DataAssetCreateMessage();
                            mes.setData(new JsonObject(dataAssetDescription.getData()));
                            mes.setDataSource(dataSource);
                            mes.setDataAssetId(initCreateReply.result());

                            dataAsset.setId(mes.getDataAssetId());
                            dataAsset.setSourceID(dataSource.getId());
                            dataAsset.setFormat(mes.getData().getString("format", null));
                            dataAsset.setName(mes.getData().getString("name", null));
                            dataAsset.setResourceID(mes.getData().getString("resourceid", null));
                            dataAsset.setUrl(mes.getData().getString("file", null));
                            dataAsset.setOrignalResourceURL(mes.getData().getString("originalurl", null));
                            dataAsset.setDatasetID(mes.getData().getString("datasetId", null));
                            dataAsset.setDatasetNotes(mes.getData().getString("datasetnotes", null));
                            dataAsset.setDatasetTitle(mes.getData().getString("datasettitle", null));
                            dataAsset.setLicenseTitle(mes.getData().getString("licensetitle", null));
                            dataAsset.setLicenseUrl(mes.getData().getString("licenseurl", null));
                            dataAsset.setOrignalDatasetURL(mes.getData().getString("originaldataseturl", null));
                            dataAsset.setOrganizationDescription(mes.getData().getString("organizationdescription", null));
                            dataAsset.setOrganizationTitle(mes.getData().getString("originalURL", null));
                            dataAsset.setTags(mes.getData().getJsonArray("tags",new JsonArray()).getList());
                            dataAsset.setVersion(mes.getData().getString("version", null));
                            dataAsset.setDataSetDescription(mes.getData().getString("datasetdescription", null));
                            dataAsset.setSignature(mes.getData().getString("signature", null));
                            dataAsset.setStatus(DataAssetStatus.APPROVED);
                            next.handle(Future.succeededFuture(dataAsset));
                        }
                        else {
                            LOGGER.error(dataSourceReply.cause());
                            next.handle(Future.failedFuture(dataSourceReply.cause()));
                        }
                    });
                }
                else {
                    dataSourceManager.findById(Integer.toUnsignedLong(dataAssetDescription.getSourceId()), dataSourceReply -> {
                        if (dataSourceReply.succeeded()) {
                            DataSource dataSource = Json.decodeValue(dataSourceReply.result().toString(), DataSource.class);

                            DataAssetCreateMessage mes = new DataAssetCreateMessage();
                            mes.setData(new JsonObject(dataAssetDescription.getData()));
                            mes.setDataSource(dataSource);
                            mes.setDataAssetId(initCreateReply.result());

                            dataSourceAdapterService.createDataAsset(dataSource.getDatasourceType(), new JsonObject(Json.encode(mes)), dataAssetCreateReply -> {
                                if (dataAssetCreateReply.succeeded()) {
                                    if (dataAssetCreateReply.result() == null) {
                                        cleanUpDataAssetDummy(next, initCreateReply.result(), dataAssetCreateReply.cause());
                                    } else {
                                        next.handle(Future.succeededFuture(Json.decodeValue(dataAssetCreateReply.result().toString(), DataAsset.class)));
                                    }
                                } else {
                                    cleanUpDataAssetDummy(next, initCreateReply.result(), dataAssetCreateReply.cause());
                                }
                            });
                        } else {
                            LOGGER.error(dataSourceReply.cause());
                            next.handle(Future.failedFuture(dataSourceReply.cause()));
                        }
                    });
                }
            } else {
                LOGGER.error(initCreateReply.cause());
                next.handle(Future.failedFuture(initCreateReply.cause()));
            }
        });
    }

    private void cleanUpDataAssetDummy(Handler<AsyncResult<DataAsset>> next, long dataAssetId, Throwable cause){
        dataAssetManager.delete(dataAssetId, deleteReply -> {
            if(deleteReply.succeeded()){
                LOGGER.error(cause);
                next.handle(Future.failedFuture(cause));
            } else{
                LOGGER.info("INCONSISTENCY IN DATABASE. There is a DataAsset object in the database with no corresponding object in the adapter.");
                next.handle(Future.failedFuture(deleteReply.cause()));
            }
        });
    }

	private void createDataAsset(long jobId, AsyncResult<DataAsset> res, String licenceurl, String licencetitle) {
		if (res.succeeded()) {
		    DataAsset dataAsset = res.result();
		    if(dataAsset.getLicenseUrl() == null){
		        dataAsset.setLicenseUrl(licenceurl);
		        dataAsset.setLicenseTitle(licencetitle);
            }
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


	public void publishAll(Handler<AsyncResult<JsonObject>> resultHandler) {
        dataAssetManager.findAll(reply -> {
            if (reply.succeeded()) {
                ArrayList<Future> publishFutures = new ArrayList<>();
                for (int i = 0; i < reply.result().size(); i++) {
                    DataAsset da = Json.decodeValue(reply.result().getJsonObject(i).toString(), DataAsset.class);
                    Future f = Future.future();
                    dataAssetManager.changeStatus(DataAssetStatus.PUBLISHED, da.getId(), f.completer());
                }
                CompositeFuture.all(publishFutures).setHandler(reply2 -> {
                    if (reply2.succeeded()) {
                        brokerController.update(reply3 -> {
                            if (reply3.succeeded()) {
                                JsonObject jO = new JsonObject();
                                jO.put("success", "Data Assets wurden veröffentlicht.");
                                resultHandler.handle(Future.succeededFuture(jO));
                            } else {
                                LOGGER.error(reply3.cause());
                                resultHandler.handle(Future.failedFuture(reply3.cause()));
                            }
                        });
                    } else {
                        LOGGER.error(reply2.cause());
                        resultHandler.handle(Future.failedFuture(reply.cause()));
                    }
                });
            } else {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    public void unpublishAll(Handler<AsyncResult<JsonObject>> resultHandler) {
        dataAssetManager.findAll(reply -> {
            if (reply.succeeded()) {
                ArrayList<Future> publishFutures = new ArrayList<>();
                for (int i = 0; i < reply.result().size(); i++) {
                    DataAsset da = Json.decodeValue(reply.result().getJsonObject(i).toString(), DataAsset.class);
                    Future f = Future.future();
                    dataAssetManager.changeStatus(DataAssetStatus.APPROVED, da.getId(), f.completer());
                }
                CompositeFuture.all(publishFutures).setHandler(reply2 -> {
                    if (reply2.succeeded()) {
                        brokerController.update(reply3 -> {
                            if (reply3.succeeded()) {
                                JsonObject jO = new JsonObject();
                                jO.put("success", "Data Assets wurden zurückgehalten.");
                                resultHandler.handle(Future.succeededFuture(jO));
                            } else {
                                LOGGER.error(reply3.cause());
                                resultHandler.handle(Future.failedFuture(reply3.cause()));
                            }
                        });
                    } else {
                        LOGGER.error(reply2.cause());
                        resultHandler.handle(Future.failedFuture(reply.cause()));
                    }
                });
            } else {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

	public void publish(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
		dataAssetManager.changeStatus(DataAssetStatus.PUBLISHED, id, reply -> {
			JsonObject jO = new JsonObject();
			if (reply.succeeded()) {
				brokerController.update(reply2 -> {
				    if(reply2.succeeded()){
                        jO.put("success", "Data Asset " + id + " wurde veröffentlicht.");
                        resultHandler.handle(Future.succeededFuture(jO));
                    } else {
                        LOGGER.error(reply.cause());
                        resultHandler.handle(Future.failedFuture(reply.cause()));
                    }
                });
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
                brokerController.update(reply2 -> {
                    if(reply2.succeeded()){
                        jO.put("success", "Data Asset " + id + " wurde zurückgehalten.");
                        resultHandler.handle(Future.succeededFuture(jO));
                    } else {
                        LOGGER.error(reply.cause());
                        resultHandler.handle(Future.failedFuture(reply.cause()));
                    }
                });
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
						Future<JsonObject> serviceDeleteFuture = Future.future();
						dataSourceAdapterService.delete(reply2.result().getString("datasourcetype"), id, serviceDeleteFuture.completer());

						Future<Void> databaseDeleteFuture = Future.future();
						dataAssetManager.delete(id, databaseDeleteFuture.completer());

						CompositeFuture.all(databaseDeleteFuture, serviceDeleteFuture).setHandler( ar -> {
							if(ar.succeeded()){
								brokerController.update(reply -> {
								    if(reply.succeeded()){
                                        JsonObject jO = new JsonObject();
                                        jO.put("status", "success");
                                        jO.put("text", "Data Asset " + id + " wurde gelöscht.");
                                        resultHandler.handle(Future.succeededFuture(jO));
                                    } else {
                                        JsonObject jO = new JsonObject();
                                        jO.put("status", "info");
                                        jO.put("text", "Data Asset " + id + " wurde gelöscht, konnte aber beim Broker nicht entfernt werden.");
                                        resultHandler.handle(Future.succeededFuture(jO));
                                    }
                                });
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
