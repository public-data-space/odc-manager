package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.messages.DataAssetCreateMessage;
import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.entities.Job;
import de.fraunhofer.fokus.ids.persistence.enums.DatasourceType;
import de.fraunhofer.fokus.ids.persistence.enums.JobStatus;
import de.fraunhofer.fokus.ids.persistence.managers.DataAssetManager;
import de.fraunhofer.fokus.ids.persistence.managers.DataSourceManager;
import de.fraunhofer.fokus.ids.persistence.managers.JobManager;
import de.fraunhofer.fokus.ids.services.webclient.WebClientService;
import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class JobService extends AbstractVerticle {

	final Logger LOGGER = LoggerFactory.getLogger(JobService.class.getName());
	private JobManager jobManager;
	private DataAssetManager dataAssetManager;
	private DataSourceManager dataSourceManager;
	private WebClientService webClientService;


	public JobService(Vertx vertx){
		jobManager = new JobManager(vertx);
		dataAssetManager = new DataAssetManager(vertx);
		dataSourceManager = new DataSourceManager(vertx);
		this.webClientService = WebClientService.createProxy(vertx, Constants.WEBCLIENT_SERVICE);

	}

	public void process(Handler<AsyncResult<Void>> resultHandler) {

		getJob(job -> initiateDataAssetCreation(da -> createDataAsset(job, da, resultHandler),
				job));
	}

	/**
	 * @param result
	 */
	private void initiateDataAssetCreation(Handler<AsyncResult<DataAsset>> next, AsyncResult<Job> result) {
		if (result.succeeded()) {
			Job job = result.result();
			LOGGER.info("Started Job with ID: " + job.getId());

			getDataSource(Long.parseLong(job.getSourceID()), reply -> {
				if (reply.succeeded()) {
					jobManager.updateStatus(job.getId(), JobStatus.RUNNING, reply2 -> {
					});

					if (job.getSourceType().equals(DatasourceType.CKAN)) {
						DataAssetCreateMessage mes = new DataAssetCreateMessage();
						mes.setJob(job);
						mes.setDataSource(reply.result());
						webClientService.post(8091, "localhost", "/create", new JsonObject(Json.encode(mes)), reply3 -> {
							if (reply3.succeeded()) {
								next.handle(Future.succeededFuture(Json.decodeValue(reply3.result().toString(), DataAsset.class)));
							} else {
								LOGGER.info(reply3.cause());
								next.handle(Future.failedFuture(reply3.cause()));
							}
						});
					} else {
					}
				} else {
					LOGGER.info(reply.cause());
					next.handle(Future.failedFuture(reply.cause()));
				}
			});
		}
		else{
			LOGGER.info("Job could not be found\n\n"+result.cause());
			next.handle(Future.failedFuture(result.cause()));
		}
	}

	private void createDataAsset(AsyncResult<Job> job, AsyncResult<DataAsset> res, Handler<AsyncResult<Void>> resultHandler) {
		if (res.succeeded() && job.succeeded()) {
			LOGGER.info("DataAsset was successfully created.");

			dataAssetManager.add(new JsonObject(Json.encode(res.result())), reply -> {
				if (reply.succeeded()) {
					LOGGER.info("DataAsset was successfully inserted to the DB.");
					jobManager.updateStatus(job.result().getId(), JobStatus.FINISHED, reply2 -> {});
					resultHandler.handle(Future.succeededFuture());
				} else {
					LOGGER.error("DataAsset insertion failed.\n\n" + res.cause());
					jobManager.updateStatus(job.result().getId(), JobStatus.ERROR, reply2 -> {});
					resultHandler.handle(Future.failedFuture(reply.cause()));
				}
			});
		} else {
			LOGGER.error("DataAsset Creation failed.\n\n" + res.cause());
			jobManager.updateStatus(job.result().getId(), JobStatus.ERROR, reply2 -> {});
			resultHandler.handle(Future.failedFuture(res.cause()));
		}
	}

	/**
	 * Method to query the correct DataSet from the database
	 * @param sourceId Id of the DataSource to be queried.
	 */
	private void getDataSource(Long sourceId, Handler<AsyncResult<DataSource>> next){
		LOGGER.info("Getting DataSource from DB.");

		dataSourceManager.findById(sourceId, reply -> {
			if(reply.succeeded()){
				LOGGER.info("DataSource retrieved.");
				next.handle(Future.succeededFuture(Json.decodeValue(reply.result().toString(), DataSource.class)));
			}
			else{
				LOGGER.error("Id could not be found.\n\n"+reply.cause());
				next.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	/**
	 * @param next
	 */
	private void getJob(Handler<AsyncResult<Job>> next) {

		jobManager.findUnfinished(reply -> {

			if (reply.succeeded()) {
				next.handle(Future.succeededFuture(Json.decodeValue(reply.result().toString(), Job.class)));
			} else {
				LOGGER.info("Job could not be found.\n\n" + reply.cause());
				next.handle(Future.failedFuture(reply.cause().toString()));
			}
		});
	}

}