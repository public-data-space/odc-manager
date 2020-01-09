package de.fraunhofer.fokus.ids.controllers;

import com.fasterxml.jackson.annotation.JsonInclude;
import de.fraunhofer.fokus.ids.enums.FileType;
import de.fraunhofer.fokus.ids.messages.ResourceRequest;
import de.fraunhofer.fokus.ids.models.*;
import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.managers.DataAssetManager;
import de.fraunhofer.fokus.ids.persistence.managers.DataSourceManager;
import de.fraunhofer.fokus.ids.services.IDSService;
import de.fraunhofer.fokus.ids.services.datasourceAdapter.DataSourceAdapterService;
import de.fraunhofer.iais.eis.ArtifactResponseMessage;
import de.fraunhofer.iais.eis.Connector;
import de.fraunhofer.iais.eis.SelfDescriptionResponse;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.StringBody;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class ConnectorController {

	private Logger LOGGER = LoggerFactory.getLogger(ConnectorController.class.getName());
	private IDSService idsService;
	private DataAssetManager dataAssetManager;
	private DataSourceManager dataSourceManager;
	private DataSourceAdapterService dataSourceAdapterService;

	public ConnectorController(Vertx vertx){
		this.idsService = new IDSService(vertx);
		this.dataAssetManager = new DataAssetManager(vertx);
		this.dataSourceManager = new DataSourceManager(vertx);
		this.dataSourceAdapterService = DataSourceAdapterService.createProxy(vertx, Constants.DATASOURCEADAPTER_SERVICE);
		Json.prettyMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
	}


	public void data(long id, String extension, Handler<AsyncResult<HttpEntity>> resultHandler){

		Future<ArtifactResponseMessage> artifactResponseFuture = Future.future();
		Future<File> fileFuture = Future.future();
		idsService.getArtifactResponse(artifactResponseFuture.completer());
		buildDataAssetReturn(id, extension, fileFuture.completer());
		CompositeFuture.all(artifactResponseFuture, fileFuture).setHandler( reply -> {
			if(reply.succeeded()) {
				MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
						.setBoundary("IDSMSGPART")
						.addTextBody("header", Json.encodePrettily(artifactResponseFuture.result()), ContentType.APPLICATION_JSON)
						.addBinaryBody("payload", fileFuture.result());

				resultHandler.handle(Future.succeededFuture(multipartEntityBuilder.build()));
			}
			else{
				LOGGER.error("Could not create response.");
			}
		});
	}

	public void payload(long id, String extension, Handler<AsyncResult<File>> resultHandler) {
		buildDataAssetReturn(id, extension, reply -> {
			if(reply.succeeded()){
				resultHandler.handle(Future.succeededFuture(reply.result()));
			}
			else{
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	private void buildDataAssetReturn(long id, String extension, Handler<AsyncResult<File>> resultHandler){
		if(extension == null) {
			payload(id, resultHandler);
		}
		else {
			payloadContent(id,extension, resultHandler);
		}
	}

	public void about(String extension, Handler<AsyncResult<ReturnObject>> resultHandler) {
		FileType type;
		try{
			type = FileType.valueOf(extension.toUpperCase());
		}
		catch(Exception e){
			type = FileType.TTL;
		}
		String contentType = getContentType(type);

		Future<Connector> connectorFuture = Future.future();
		idsService.getConnector(connectorFuture.completer());
		Future<SelfDescriptionResponse> responseFuture = Future.future();
		idsService.getSelfDescriptionResponse(responseFuture.completer());

		CompositeFuture.all(connectorFuture,responseFuture).setHandler( reply -> {
			if (reply.succeeded()) {
				ContentBody cb = new StringBody(Json.encodePrettily(responseFuture.result()), ContentType.create("application/json"));
				ContentBody result = new StringBody(Json.encodePrettily(connectorFuture.result()), ContentType.create("application/json"));

				MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
						.setBoundary("IDSMSGPART")
						.setCharset(StandardCharsets.UTF_8)
						.setContentType(ContentType.APPLICATION_JSON)
						.addPart("header", cb)
						.addPart("payload", result);

				ByteArrayOutputStream out = new ByteArrayOutputStream();
				try {
					multipartEntityBuilder.build().writeTo(out);
				} catch (IOException e) {
					LOGGER.error(e);
					resultHandler.handle(Future.failedFuture(e.getMessage()));
				}
				resultHandler.handle(Future.succeededFuture(new ReturnObject(out.toString(), contentType)));
			}
			else {
				LOGGER.error("Connector Object could not be retrieved.",reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	private void payload(Long id, Handler<AsyncResult<File>> resultHandler) {
		getPayload(id, FileType.MULTIPART, resultHandler);
	}

	private void payloadContent(Long id, String extension, Handler<AsyncResult<File>> resultHandler) {
		if(extension.equals("json")) {
			getPayload(id, FileType.JSON, resultHandler);
		}
		if(extension.equals("txt")) {
			getPayload(id, FileType.TXT, resultHandler);
		}
		getPayload(id, FileType.MULTIPART, resultHandler);
	}

	private void getPayload(Long id, FileType fileType, Handler<AsyncResult<File>> resultHandler) {

		dataAssetManager.findById(id, reply -> {
			if (reply.succeeded()) {
				DataAsset dataAsset = Json.decodeValue(reply.result().toString(), DataAsset.class);

				dataSourceManager.findById(dataAsset.getSourceID(), reply2 -> {
					if(reply2.succeeded()){
						DataSource dataSource = Json.decodeValue(reply2.result().toString(), DataSource.class);

						ResourceRequest request = new ResourceRequest();
						request.setDataSource(dataSource);
						request.setDataAsset(dataAsset);
						request.setFileType(fileType);

						dataSourceAdapterService.getFile(dataSource.getDatasourceType(), new JsonObject(Json.encode(request)), reply3 -> {
							if(reply3.succeeded()){
								resultHandler.handle(Future.succeededFuture(new File(reply3.result())));
							}
							else{
								LOGGER.error("FileContent could not be retrieved.",reply3.cause());
								resultHandler.handle(Future.failedFuture(reply3.cause()));
							}
						});
					}
					else{
						LOGGER.error("DataAsset could not be retrieved.",reply2.cause());
						resultHandler.handle(Future.failedFuture(reply2.cause()));
					}
				});
			}
			else {
				LOGGER.error("DataAsset could not be read.",reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	private String getContentType(FileType fileType) {
		if(fileType.equals(FileType.TTL)) {
			return "text/turtle";
		}
		if(fileType.equals(FileType.JSONLD)) {
			return "application/ld+json";
		}
		if(fileType.equals(FileType.RDF)) {
			return "application/rdf+xml";
		}
		return "text/plain; charset=utf-8";
	}
}