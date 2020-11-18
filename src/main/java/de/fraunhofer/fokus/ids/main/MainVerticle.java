package de.fraunhofer.fokus.ids.main;

import de.fraunhofer.fokus.ids.controllers.*;
import de.fraunhofer.fokus.ids.models.DataAssetDescription;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.managers.AuthManager;
import de.fraunhofer.fokus.ids.persistence.managers.BrokerManager;
import de.fraunhofer.fokus.ids.persistence.util.DatabaseConnector;
import de.fraunhofer.fokus.ids.services.ConfigService;
import de.fraunhofer.fokus.ids.services.InitService;
import de.fraunhofer.fokus.ids.services.authAdapter.AuthAdapterServiceVerticle;
import de.fraunhofer.fokus.ids.services.brokerService.BrokerServiceVerticle;
import de.fraunhofer.fokus.ids.services.datasourceAdapter.DataSourceAdapterServiceVerticle;
import de.fraunhofer.fokus.ids.services.dockerService.DockerServiceVerticle;
import de.fraunhofer.fokus.ids.utils.IDSMessageParser;
import de.fraunhofer.iais.eis.ArtifactRequestMessage;
import de.fraunhofer.iais.eis.DescriptionRequestMessage;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.JWTAuthHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;

import java.io.*;
import java.util.*;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class MainVerticle extends AbstractVerticle{
	private Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class.getName());
	private AuthManager authManager;
	private ConnectorController connectorController;
	private DataAssetController dataAssetController;
	private DataSourceController dataSourceController;
	private JobController jobController;
	private BrokerController brokerController;
	private BrokerManager brokerManager;
	private ConfigService configService;
	private int servicePort;
	private DockerController dockerController;
	private FileUploadController fileUploadController;

	@Override
	public void start(Promise<Void> startPromise) {
		this.authManager = new AuthManager(vertx);
		this.connectorController = new ConnectorController(vertx);
		this.dataAssetController = new DataAssetController(vertx);
		this.dataSourceController = new DataSourceController(vertx);
		this.fileUploadController = new FileUploadController(vertx);
		this.jobController = new JobController(vertx);
		this.brokerController = new BrokerController(vertx);
		this.brokerManager = new BrokerManager();
		this.configService = new ConfigService(vertx);
		this.dockerController = new DockerController(vertx);

		DeploymentOptions deploymentOptions = new DeploymentOptions();
		deploymentOptions.setWorker(true);

		LOGGER.info("Starting services...");
		Future<String> deployment = Future.succeededFuture();
		deployment
				.compose(id2 -> {
					Promise<String> datasourceAdapterPromise = Promise.promise();
					Future<String> datasourceAdapterFuture = datasourceAdapterPromise.future();
					vertx.deployVerticle(DataSourceAdapterServiceVerticle.class.getName(), deploymentOptions, datasourceAdapterFuture);
					return datasourceAdapterFuture;
				})
				.compose(id3 -> {
					Promise<String> dockerServiceAdapterPromise = Promise.promise();
					Future<String> dockerServiceAdapterFuture = dockerServiceAdapterPromise.future();
					vertx.deployVerticle(DockerServiceVerticle.class.getName(), deploymentOptions, dockerServiceAdapterFuture);
					return dockerServiceAdapterFuture;
				})
				.compose(i45 -> {
					Promise<String> authAdapterPromise = Promise.promise();
					Future<String> authAdapterFuture = authAdapterPromise.future();
					vertx.deployVerticle(AuthAdapterServiceVerticle.class.getName(), deploymentOptions, authAdapterFuture);
					return authAdapterFuture;
				})
				.compose(id5 -> {
					Promise<String> brokerServicePromise = Promise.promise();
					Future<String> brokerServiceFuture = brokerServicePromise.future();
					vertx.deployVerticle(BrokerServiceVerticle.class.getName(), deploymentOptions, brokerServiceFuture);
					return brokerServiceFuture;
				})
				.compose(id6 -> {
					Promise<String> envPromise = Promise.promise();
					ConfigStoreOptions confStore = new ConfigStoreOptions()
							.setType("env");
					ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(confStore);
					ConfigRetriever retriever = ConfigRetriever.create(vertx, options);
					retriever.getConfig(ar -> {
						if (ar.succeeded()) {
							servicePort = ar.result().getInteger("SERVICE_PORT");
							DatabaseConnector.getInstance().create(vertx, ar.result().getJsonObject("DB_CONFIG"), 5);
							envPromise.complete();
						} else {
							envPromise.fail(ar.cause());
						}
					});
					return envPromise.future();
				}).onComplete( ar -> {
					if (ar.succeeded()) {
						InitService initService = new InitService(vertx);
						initService.initDatabase(reply -> {
							if(reply.succeeded()){
								createHttpServer(vertx);
								startPromise.complete();
							}
							else{
								LOGGER.error(reply.cause());
								startPromise.fail(ar.cause());
							}
						});
					} else {
						LOGGER.error(ar.cause());
						startPromise.fail(ar.cause());
					}
				});

	}

	private void createHttpServer(Vertx vertx) {

		OpenAPI3RouterFactory.create(vertx, "/webroot/swagger.yaml", ar -> {
					if (ar.succeeded()) {
						OpenAPI3RouterFactory routerFactory = ar.result();

						routerFactory.addSecurityHandler("bearerAuth", JWTAuthHandler.create(authManager.getProvider()));

						routerFactory
								.addHandlerByOperationId("loginId",routingContext ->
										authManager.login(routingContext.getBodyAsJson(), reply -> {
											if(reply.succeeded()) {
												if (reply.result() != null) {
													routingContext.response().end(reply.result());
												} else {
													routingContext.fail(401);
												}
											}
											else{
												routingContext.response().setStatusCode(500).end();
											}
										})
								)
								.addHandlerByOperationId("aboutPostId",routingContext ->
										connectorController.checkMessage(IDSMessageParser.parse(routingContext.request().formAttributes()), DescriptionRequestMessage.class, result ->
										replyMessage(result, routingContext.response())))
								.addHandlerByOperationId("aboutGetId",routingContext ->
										connectorController.about(result ->
										reply(result, routingContext.response())))
								.addHandlerByOperationId("dataPostId",routingContext ->
										connectorController.checkMessage(IDSMessageParser.parse(routingContext.request().formAttributes()), ArtifactRequestMessage.class, result ->
										replyMessage(result, routingContext.response())))
								.addHandlerByOperationId("dataGetId",routingContext ->
										connectorController.payload(Long.parseLong(routingContext.request().getParam("id")), "", result ->
										replyFile(result,Long.parseLong(routingContext.request().getParam("id")), routingContext.response())))
								.addHandlerByOperationId("infrastructureId",routingContext ->
										connectorController.routeMessage(IDSMessageParser.parse(routingContext.request().formAttributes()), result ->
										replyMessage(result, routingContext.response())))

								// Jobs

								.addHandlerByOperationId("jobGetId",routingContext ->
										jobController.findAll(result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("jobDeleteId",routingContext ->
										jobController.deleteAll(result -> reply(result, routingContext.response())))

								// Data Assets

								.addHandlerByOperationId("getCountsId",routingContext ->
										dataAssetController.counts(result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("publishAllDataAssetsId",routingContext ->
										dataAssetController.publishAll(result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("unpublishAllDataAssetsId",routingContext ->
										dataAssetController.unpublishAll(result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("publishDataAssetId",routingContext ->
										dataAssetController.publish(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("unpublishDataAssetId",routingContext ->
										dataAssetController.unPublish(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("deleteDataAssetId",routingContext ->
										dataAssetController.delete(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("getDataAssetsId",routingContext ->
										dataAssetController.index(result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("addDataAssetId",routingContext ->
										processDataAssetInformation(routingContext))

								// Data Sources

								.addHandlerByOperationId("dataSourceAddId",routingContext ->
										dataSourceController.add(toDataSource(routingContext.getBodyAsJson()), result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("dataSourceDeleteId",routingContext ->
										dataSourceController.delete(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("allDataSourceGetId",routingContext ->
										dataSourceController.findAllByType(result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("datasourceGetId",routingContext ->
										dataSourceController.findById(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("dataSourceTypeGetId",routingContext ->
										dataSourceController.findByType(routingContext.request().getParam("type"), result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("dataSourceEditId",routingContext ->
										dataSourceController.update(toDataSource(routingContext.getBodyAsJson()),Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("dataSourceSchemaGetId",routingContext ->
										dataSourceController.getFormSchema(routingContext.request().getParam("type"), result -> reply(result, routingContext.response())))

								// Broker

								.addHandlerByOperationId("brokerAddId",routingContext ->
										brokerController.add(routingContext.getBodyAsJson().getString("url"), result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("brokerUnregisterId",routingContext ->
										brokerController.unregister(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("brokerRegisterId",routingContext ->
										brokerController.register(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("brokerGetId",routingContext ->
										brokerManager.findAll(result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("brokerDeleteId",routingContext ->
										brokerController.delete(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))

								// Config

								.addHandlerByOperationId("configGetId",routingContext ->
										configService.getConfiguration(result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("configEditId",routingContext ->
										configService.editConfiguration(routingContext.getBodyAsJson(), result -> reply(result, routingContext.response())))

								// Upload

								.addHandlerByOperationId("fileAddId",routingContext ->
										fileUploadController.uploadFile(routingContext, result -> reply(result, routingContext.response())))

								// Adapters

								.addHandlerByOperationId("adapterGetId",routingContext ->
										dataSourceController.listAdapters(result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("imagesGetId",routingContext ->
										dockerController.getImages(result -> reply(result, routingContext.response())))
								.addHandlerByOperationId("adapterStartId",routingContext ->
										dockerController.startImages(routingContext.getBodyAsString(), reply -> reply(reply, routingContext.response())))
								.addHandlerByOperationId("adapterStopId",routingContext ->
										dockerController.stopImages(routingContext.getBodyAsString(), reply -> reply(reply, routingContext.response())))
						;

						Set<String> allowedHeaders = new HashSet<>();
						allowedHeaders.add("x-requested-with");
						allowedHeaders.add("Access-Control-Allow-Origin");
						allowedHeaders.add("Access-Control-Allow-Credentials");
						allowedHeaders.add("origin");
						allowedHeaders.add("authorization");
						allowedHeaders.add("Content-Type");
						allowedHeaders.add("accept");
						allowedHeaders.add("Access-Control-Allow-Headers");
						allowedHeaders.add("Access-Control-Allow-Methods");
						allowedHeaders.add("X-PINGARUNER");

						Set<HttpMethod> allowedMethods = new HashSet<>();
						allowedMethods.add(HttpMethod.GET);
						allowedMethods.add(HttpMethod.POST);
						allowedMethods.add(HttpMethod.OPTIONS);

						Router router = routerFactory.getRouter();
						router.route("/").handler(routingContext -> {
							connectorController.about(result ->
									reply(result, routingContext.response()));
						});
						router.route().handler(CorsHandler.create("*").allowedHeaders(allowedHeaders).allowedMethods(allowedMethods));
						router.route().handler(BodyHandler.create());
						router.route("/openapi*").handler(StaticHandler.create());

						HttpServer server = vertx.createHttpServer();
						server.requestHandler(router).listen(this.servicePort);
						LOGGER.info("odc-manager deployed on port "+servicePort);

					} else {
						LOGGER.error(ar.cause());
					}
				});
	}

	private void processDataAssetInformation(RoutingContext routingContext){
		JsonObject jsonObject = routingContext.getBodyAsJson();
		String licenseurl = jsonObject.getString("licenseurl");
		String licensetitle = jsonObject.getString("licensetitle");
		jsonObject.remove("licenseurl");
		jsonObject.remove("licensetitle");
		dataAssetController.add(Json.decodeValue(jsonObject.toString(), DataAssetDescription.class), licenseurl, licensetitle, result -> reply(result, routingContext.response()));
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
			response.setStatusCode(500).end();
		}
	}
	private void reply(AsyncResult result, HttpServerResponse response){
		if(result.succeeded()){
			reply(result.result(), response);
		}
		else{
			LOGGER.error("Result Future failed.",result.cause());
			response.setStatusCode(500).end();
		}
	}

	private void replyFile(AsyncResult<File> result,Long id, HttpServerResponse response){
		dataAssetController.getFileName(id,stringAsyncResult->{
		    if (stringAsyncResult.succeeded()){
                if(result.succeeded()){
                    if(result.result() != null) {
                        response.putHeader(HttpHeaders.CONTENT_DISPOSITION,"attachment; filename=\""+stringAsyncResult.result()+"\"");
                        response.sendFile(result.result().toString());
                        new File(result.result().toString()).delete();
                    } else {
						LOGGER.error("Dataset not found! ",stringAsyncResult.cause());
						response.setStatusCode(500).end();
					}
                }
                else{
                    LOGGER.error("Result Future failed.",result.cause());
                    response.setStatusCode(500).end();
                }
            }
		    else {
                LOGGER.error("Filename not found! ",stringAsyncResult.cause());
                response.setStatusCode(500).end();
            }
        });

	}

	private void replyMessage(AsyncResult<HttpEntity> result, HttpServerResponse response){
		if(result.succeeded()){
			if(result.result() != null) {
				try(ByteArrayOutputStream baos = new ByteArrayOutputStream()){
					Header contentTypeHeader =  result.result().getContentType();
					result.result().writeTo(baos);
					response.putHeader(contentTypeHeader.getName(), contentTypeHeader.getValue());
					response.end(Buffer.buffer(baos.toByteArray()));
				} catch (IOException e) {
					LOGGER.error(e);
					response.setStatusCode(500);
					response.end();
				}
			}
		}
		else{
			LOGGER.error("Result Future failed.",result.cause());
			response.setStatusCode(500).end();
		}
	}

	public static void main(String[] args) {
		String[] params = Arrays.copyOf(args, args.length + 1);
		params[params.length - 1] = MainVerticle.class.getName();
		Launcher.executeCommand("run", params);
	}
}