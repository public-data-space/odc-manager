package de.fraunhofer.fokus.ids.controllers;

import de.fraunhofer.fokus.ids.enums.FileType;
import de.fraunhofer.fokus.ids.messages.ResourceRequest;
import de.fraunhofer.fokus.ids.models.*;
import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.enums.DatasourceType;
import de.fraunhofer.fokus.ids.persistence.managers.DataAssetManager;
import de.fraunhofer.fokus.ids.persistence.managers.DataSourceManager;
import de.fraunhofer.fokus.ids.services.IDSService;
import de.fraunhofer.fokus.ids.services.webclient.WebClientService;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class ConnectorController {

	private Logger LOGGER = LoggerFactory.getLogger(ConnectorController.class.getName());
	private IDSService idsService;
	private DataAssetManager dataAssetManager;
	private DataSourceManager dataSourceManager;
	private WebClientService webClientService;

	public ConnectorController(Vertx vertx){
		this.idsService = new IDSService(vertx);
		this.dataAssetManager = new DataAssetManager(vertx);
		this.dataSourceManager = new DataSourceManager(vertx);
		this.webClientService = WebClientService.createProxy(vertx, Constants.WEBCLIENT_SERVICE);
	}

	public void data(DataRequest dataRequest, Handler<AsyncResult<ReturnObject>> resultHandler) {
		if(dataRequest.getExtension() == null) {
			payload(Long.valueOf(dataRequest.getId()), resultHandler);
		}
		else {
			payloadContent(Long.valueOf(dataRequest.getId()), dataRequest.getExtension(), resultHandler);
		}
	}

	public void about(String extension, Handler<AsyncResult<ReturnObject>> resultHandler) {
		ContentTypeWrapper contentTypeWrapper = getContentTypeWrapper(FileType.valueOf(extension.toUpperCase()));
		idsService.getConnector( reply -> {
			if (reply.succeeded()) {
				ContentBody cb = new StringBody(Json.encodePrettily(idsService.getSelfDescriptionResponse()), ContentType.create("application/json"));
				ContentBody result = new StringBody(Json.encodePrettily(reply.result()), ContentType.create("application/json"));

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
					LOGGER.error("",e);
					resultHandler.handle(Future.failedFuture(e.getMessage()));
				}
				resultHandler.handle(Future.succeededFuture(new ReturnObject(out.toString(), contentTypeWrapper)));
			}
			else {
				LOGGER.error("Connector Object could not be retrieved.\n\n"+reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	private void payload(Long id, Handler<AsyncResult<ReturnObject>> resultHandler) {
		getPayload(id, FileType.MULTIPART, resultHandler);
	}

	private void payloadContent(Long id, String extension, Handler<AsyncResult<ReturnObject>> resultHandler) {
		if(extension.equals("json")) {
			getPayload(id, FileType.JSON, resultHandler);
		}
		if(extension.equals("txt")) {
			getPayload(id, FileType.TXT, resultHandler);
		}
		getPayload(id, FileType.MULTIPART, resultHandler);
	}

	private void getPayload(Long id, FileType fileType, Handler<AsyncResult<ReturnObject>> resultHandler) {

		dataAssetManager.findById(id, reply -> {
			if (reply.succeeded()) {
				DataAsset dataAsset = Json.decodeValue(reply.result().toString(), DataAsset.class);

				dataSourceManager.findById(Long.parseLong(dataAsset.getSourceID()), reply2 -> {
					if(reply2.succeeded()){
						DataSource dataSource = Json.decodeValue(reply2.result().toString(), DataSource.class);

						ResourceRequest request = new ResourceRequest();
						request.setDataSource(dataSource);
						request.setDataAsset(dataAsset);
						request.setFileType(fileType);

						if(dataSource.getDatasourceType().equals(DatasourceType.CKAN)){


							webClientService.post(8091,"localhost","/getFile", new JsonObject(Json.encode(request)), reply3 -> {
								if(reply3.succeeded()){
									if(fileType.equals(FileType.JSON)) {
										getJSON(reply3.result().getString("result"), resultHandler);
									} else {
										getMultiPart(reply3.result().toString(), fileType, resultHandler);
									}
								}
								else{
									LOGGER.info("FileContent could not be retrieved.\n\n"+reply3.cause());
									resultHandler.handle(Future.failedFuture(reply3.cause()));
								}
							});
						}
						else{
							//TODO Postgres
						}
					}
					else{
						LOGGER.info("DataAsset could not be retrieved.\n\n"+reply2.cause());
						resultHandler.handle(Future.failedFuture(reply2.cause()));
					}
				});
			}
			else {
				LOGGER.error("DataAsset could not be read.\n\n"+reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	private void getJSON(String fileContent, Handler<AsyncResult<ReturnObject>> resultHandler) {
		IDSMetadata idsMetadata = new IDSMetadata();
		String cb = Json.encode(idsService.getSelfDescriptionResponse());
		idsMetadata.header = cb;
		idsMetadata.payload = fileContent;
		ReturnObject returnObject = new ReturnObject(Json.encode(idsMetadata), new ContentTypeWrapper("application/json", ""));
		resultHandler.handle(Future.succeededFuture(returnObject));
	}

	private void getMultiPart(String fileContent, FileType fileType, Handler<AsyncResult<ReturnObject>> resultHandler) {
		ContentBody	cb = new StringBody(Json.encode(idsService.getSelfDescriptionResponse()), ContentType.create("application/json"));
		MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
				.setBoundary("IDSMSGPART")
				.setCharset(StandardCharsets.UTF_8)
				.addPart("header", cb)
				.addTextBody("payload", fileContent, ContentType.create("text/plain", StandardCharsets.UTF_8));

		if(fileType.equals(FileType.MULTIPART)) {
			multipartEntityBuilder.setContentType(ContentType.create("multipart/mixed"));
		}
		if(fileType.equals(FileType.TXT)) {
			multipartEntityBuilder.setContentType(ContentType.create("text/plain"));
		}

		HttpEntity data = multipartEntityBuilder.build();
		OutputStream out = new ByteArrayOutputStream();
		try {
			data.writeTo(out);
		} catch (IOException e) {
			LOGGER.error("Exception", e);
			resultHandler.handle(Future.failedFuture(e.getMessage()));
		}
		ReturnObject returnObject = new ReturnObject(out.toString(), new ContentTypeWrapper(data.getContentType().getValue(),""));
		resultHandler.handle(Future.succeededFuture(returnObject));
	}

	private ContentTypeWrapper getContentTypeWrapper(FileType fileType) {
		if(fileType.equals(FileType.TTL)) {
			return new ContentTypeWrapper("text/turtle", "TTL");
		}
		if(fileType.equals(FileType.JSONLD)) {
			return new ContentTypeWrapper("application/ld+json", "JSON-LD");
		}
		if(fileType.equals(FileType.RDF)) {
			return new ContentTypeWrapper("application/rdf+xml", "RDF/XML");
		}
		return new ContentTypeWrapper("text/plain; charset=utf-8", "TTL");
	}
}