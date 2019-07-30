package de.fraunhofer.fokus.ids.main;

import de.fraunhofer.fokus.ids.controllers.ConnectorController;
import de.fraunhofer.fokus.ids.controllers.DataAssetController;
import de.fraunhofer.fokus.ids.controllers.DataSourceController;
import de.fraunhofer.fokus.ids.controllers.JobController;
import de.fraunhofer.fokus.ids.models.DataAssetDescription;
import de.fraunhofer.fokus.ids.models.DataRequest;
import de.fraunhofer.fokus.ids.models.ReturnObject;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.managers.AuthManager;
import de.fraunhofer.fokus.ids.persistence.service.DatabaseServiceVerticle;
import de.fraunhofer.fokus.ids.services.datasourceAdapter.DataSourceAdapterServiceVerticle;
import io.vertx.core.*;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import org.apache.http.entity.ContentType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class MainVerticle extends AbstractVerticle{
	private Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class.getName());
	private Router router;
	private AuthManager authManager;
	private ConnectorController connectorController;
	private DataAssetController dataAssetController;
	private DataSourceController dataSourceController;
	private JobController jobController;

	@Override
	public void start(Future<Void> startFuture) {
		this.authManager = new AuthManager(vertx);
		this.connectorController = new ConnectorController(vertx);
		this.dataAssetController = new DataAssetController(vertx);
		this.dataSourceController = new DataSourceController(vertx);
		this.jobController = new JobController(vertx);

		DeploymentOptions deploymentOptions = new DeploymentOptions();
		deploymentOptions.setWorker(true);

		vertx.deployVerticle( DatabaseServiceVerticle.class.getName(),deploymentOptions,  reply-> LOGGER.info("DataBaseService started"));
		vertx.deployVerticle( DataSourceAdapterServiceVerticle.class.getName(),deploymentOptions, reply-> {
			if(reply.succeeded()){
				LOGGER.info("DataSourceAdapterService started");
			}
			else{
				LOGGER.info(reply.cause());
			}
		} );

		router = Router.router(vertx);
		createHttpServer(vertx);

	}

	private void createHttpServer(Vertx vertx) {
		HttpServer server = vertx.createHttpServer();

		Set<String> allowedHeaders = new HashSet<>();
		allowedHeaders.add("x-requested-with");
		allowedHeaders.add("Access-Control-Allow-Origin");
		allowedHeaders.add("origin");
		allowedHeaders.add("Content-Type");
		allowedHeaders.add("accept");
		allowedHeaders.add("X-PINGARUNER");

		Set<HttpMethod> allowedMethods = new HashSet<>();
		allowedMethods.add(HttpMethod.GET);
		allowedMethods.add(HttpMethod.POST);
		allowedMethods.add(HttpMethod.OPTIONS);
		/*
		 * these methods aren't necessary for this sample,
		 * but you may need them for your projects
		 */
		allowedMethods.add(HttpMethod.DELETE);
		allowedMethods.add(HttpMethod.PATCH);
		allowedMethods.add(HttpMethod.PUT);

		router.route().handler(CorsHandler.create("*").allowedHeaders(allowedHeaders).allowedMethods(allowedMethods));
		router.route().handler(BodyHandler.create());

		router.route("/").handler(routingContext -> reply("Hello World!", routingContext.response()));

		router.post("/login").handler(routingContext ->
				authManager.login(routingContext.getBodyAsJson(), reply -> {
					if(reply.succeeded()) {
						if (reply.result() != null) {
							routingContext.response().end(reply.result());
						} else {
							routingContext.fail(401);
						}
					}
					else{
						routingContext.response().setStatusCode(404).end();
					}
				})
		);

		router.route("/about/").handler(routingContext ->
				connectorController.about("",result ->
						replyWithContentType(result, routingContext.response())));

		router.route("/about/:extension").handler(routingContext ->
				connectorController.about(routingContext.request().getParam("extension"), result ->
						replyWithContentType(result, routingContext.response())));

		router.route("/data/:id.:extension").handler(routingContext ->
				connectorController.data(new DataRequest(routingContext.request().getParam("id"), routingContext.request().getParam("extension")), result ->
					replyWithContentType(result, routingContext.response())));

		router.route("/data/:id").handler(routingContext ->
				connectorController.data(new DataRequest(routingContext.request().getParam("id"), ""), result ->
					replyWithContentType(result, routingContext.response())));

		router.route("/jobs/find/all").handler(routingContext ->
				jobController.findAll(result -> reply(result, routingContext.response())));

		router.route("/jobs/delete/all").handler(routingContext ->
				jobController.deleteAll(result -> reply(result, routingContext.response())));

		router.route("/dataassets/:id/publish").handler(routingContext ->
				dataAssetController.publish(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())));

		router.route("/dataassets/:id/unpublish").handler(routingContext ->
				dataAssetController.unPublish(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())));


		router.route("/dataassets/:id/delete").handler(routingContext ->
				dataAssetController.delete(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())));

		router.route("/dataassets/").handler(routingContext ->
				dataAssetController.index(result -> reply(result, routingContext.response())));


		router.route("/dataassets/counts/").handler(routingContext ->
				dataAssetController.counts(result -> reply(result, routingContext.response())));


//		router.route("/dataassets/resource/:name").handler(routingContext ->
//				dataAssetController.resource("",result -> reply(result, routingContext.response())));

		router.post("/dataassets/add").handler(routingContext ->
				dataAssetController.add(Json.decodeValue(routingContext.getBodyAsJson().toString(), DataAssetDescription.class), result -> reply(result, routingContext.response())));

//		router.route("/uri/:name").handler(routingContext ->
//						getUri(result -> reply(result, routingContext.response()), routingContext)
//				);

		router.post("/datasources/add/").handler(routingContext -> {
				dataSourceController.add(toDataSource(routingContext.getBodyAsJson()), result -> reply(result, routingContext.response()));
				});

		router.route("/datasources/delete/:id").handler(routingContext ->
				dataSourceController.delete(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())));

		router.route("/datasources/findAll").handler(routingContext ->
				dataSourceController.findAllByType(result -> reply(result, routingContext.response())));

		router.route("/datasources/find/id/:id").handler(routingContext ->
				dataSourceController.findById(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())));

		router.post("/datasources/edit/").handler(routingContext ->
				dataSourceController.update(Json.decodeValue(routingContext.getBodyAsJson().toString(), DataSource.class), result -> reply(result, routingContext.response())));

		server.requestHandler(router).listen(8090);
		LOGGER.info("odc-manager deployed on localhost:8090");
	}

	//TODO: WORKAROUND. Find way to use Json.deserialize()
	private DataSource toDataSource(JsonObject bodyAsJson) {
		DataSource ds = new DataSource();
		ds.setData(bodyAsJson.getJsonObject("data"));
		ds.setDatasourceName(bodyAsJson.getString("datasourcename"));
		ds.setDatasourceType(bodyAsJson.getString("datasourcetype"));
		return ds;
	}

	private void reply(Object result, HttpServerResponse response) {
		if (result != null) {
			String entity = result.toString();
			response.putHeader("content-type", ContentType.APPLICATION_JSON.toString());
			response.end(entity);
		} else {
			response.setStatusCode(404).end();
		}
	}
	private void reply(AsyncResult result, HttpServerResponse response){
		if(result.succeeded()){
			reply(result.result(), response);
		}
		else{
			LOGGER.error("Result Future failed.\n\n"+result.cause());
			response.setStatusCode(404).end();
		}
	}

		private void replyWithContentType(AsyncResult<ReturnObject> result, HttpServerResponse response){
		if (result.succeeded()) {
			if(result.result() != null) {
				ReturnObject returnObject = result.result();
				String entity = returnObject.getEntity();
				response.putHeader("content-type", returnObject.getTypeWrapper().getContentType());
				response.end(entity);
			}
			else{
				LOGGER.error("Resultbody was empty.");
				response.setStatusCode(404).end();
			}
		}
		else {
			LOGGER.error("Result Future failed.\n\n"+result.cause());
			response.setStatusCode(404).end();
		}
	}

	public static void main(String[] args) {
		String[] params = Arrays.copyOf(args, args.length + 1);
		params[params.length - 1] = MainVerticle.class.getName();
		Launcher.executeCommand("run", params);
	}
}