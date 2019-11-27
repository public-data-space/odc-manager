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
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
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

			dataAssetManager.addInitial(initCreateReply -> {
				if(initCreateReply.succeeded()){
					Job job = result.result();
					LOGGER.info("Started Job with ID: " + job.getId());

					getDataSource(Long.parseLong(job.getSourceID()), dataSourceReply -> {
						if (dataSourceReply.succeeded()) {
							jobManager.updateStatus(job.getId(), JobStatus.RUNNING, statusUpdateReply -> {});

							DataAssetCreateMessage mes = new DataAssetCreateMessage();
							mes.setJob(job);
							mes.setDataSource(dataSourceReply.result());
							mes.setDataAssetId(initCreateReply.result());
							dataSourceAdapterService.createDataAsset(dataSourceReply.result().getDatasourceType(), new JsonObject(Json.encode(mes)), dataAssetCreateReply-> {
								if (dataAssetCreateReply.succeeded()) {
									if (dataAssetCreateReply.result()==null){
										dataAssetManager.delete(initCreateReply.result(), deleteReply -> {
											if(deleteReply.succeeded()){
												LOGGER.error(dataAssetCreateReply.cause());
												next.handle(Future.failedFuture(dataAssetCreateReply.cause()));										}
											else{
												LOGGER.info("INCONSISTENCY IN DATABASE.");
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
		else{
			LOGGER.error("Job could not be found",result.cause());
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
					LOGGER.error("DataAsset insertion failed.", res.cause());
					jobManager.updateStatus(job.result().getId(), JobStatus.ERROR, reply2 -> {});
					resultHandler.handle(Future.failedFuture(reply.cause()));
				}
			});
		} else {
			LOGGER.error("DataAsset Creation failed.", res.cause());
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
				LOGGER.error("Id could not be found.", reply.cause());
				next.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	private void getJob(Handler<AsyncResult<Job>> next) {

		jobManager.findUnfinished(reply -> {

			if (reply.succeeded()) {
				next.handle(Future.succeededFuture(Json.decodeValue(reply.result().toString(), Job.class)));
			} else {
				LOGGER.error("Job could not be found.", reply.cause());
				next.handle(Future.failedFuture(reply.cause().toString()));
			}
		});
	}

}