package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.messages.DataAssetCreateMessage;
import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.entities.Job;
import de.fraunhofer.fokus.ids.persistence.enums.JobStatus;
import de.fraunhofer.fokus.ids.persistence.managers.DataAssetManager;
import de.fraunhofer.fokus.ids.persistence.managers.DataSourceManager;
import de.fraunhofer.fokus.ids.persistence.managers.JobManager;
import de.fraunhofer.fokus.ids.services.datasourceAdapter.DataSourceAdapterService;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class JobService extends AbstractVerticle {

	private final Logger LOGGER = LoggerFactory.getLogger(JobService.class.getName());
	private JobManager jobManager;
	private DataAssetManager dataAssetManager;
	private DataSourceManager dataSourceManager;
	private DataSourceAdapterService dataSourceAdapterService;


	public JobService(Vertx vertx){
		this.jobManager = new JobManager(vertx);
		this.dataAssetManager = new DataAssetManager(vertx);
		this.dataSourceManager = new DataSourceManager(vertx);
		this.dataSourceAdapterService = DataSourceAdapterService.createProxy(vertx, Constants.DATASOURCEADAPTER_SERVICE);

	}

	public void process(Handler<AsyncResult<Void>> resultHandler) {

		getJob(job -> initiateDataAssetCreation(da -> createDataAsset(job, da, resultHandler),
				job));
	}

	private void initiateDataAssetCreation(Handler<AsyncResult<DataAsset>> next, AsyncResult<Job> result) {
		if (result.succeeded()) {

			dataAssetManager.addInitial(reply -> {
				if(reply.succeeded()){
					Job job = result.result();
					LOGGER.info("Started Job with ID: " + job.getId());

					getDataSource(Long.parseLong(job.getSourceID()), reply4 -> {
						if (reply4.succeeded()) {
							jobManager.updateStatus(job.getId(), JobStatus.RUNNING, reply2 -> {});

							DataAssetCreateMessage mes = new DataAssetCreateMessage();
							mes.setJob(job);
							mes.setDataSource(reply4.result());
							mes.setDataAssetId(reply.result());
							dataSourceAdapterService.createDataAsset(reply4.result().getDatasourceType(), new JsonObject(Json.encode(mes)), reply5-> {
								if (reply5.succeeded()) {
									next.handle(Future.succeededFuture(Json.decodeValue(reply5.result().toString(), DataAsset.class)));
								} else {
									LOGGER.info(reply5.cause());
									next.handle(Future.failedFuture(reply5.cause()));
								}
							});
						} else {
							LOGGER.info(reply4.cause());
							next.handle(Future.failedFuture(reply4.cause()));
						}
					});
				}
				else{
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