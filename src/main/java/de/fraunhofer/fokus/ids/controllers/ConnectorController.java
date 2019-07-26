package de.fraunhofer.fokus.ids.controllers;

import de.fraunhofer.fokus.ids.models.IDSMetadata;
import de.fraunhofer.fokus.ids.models.ReturnObject;
import de.fraunhofer.fokus.ids.persistence.managers.DataAssetManager;
import de.fraunhofer.fokus.ids.services.IDSService;
import de.fraunhofer.fokus.ids.models.ContentTypeWrapper;
import de.fraunhofer.fokus.ids.models.DataRequest;
import io.vertx.core.*;
import io.vertx.core.json.Json;
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

	public ConnectorController(Vertx vertx){
		this.idsService = new IDSService(vertx);
		this.dataAssetManager = new DataAssetManager(vertx);
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
		ContentTypeWrapper contentTypeWrapper = getContentTypeWrapper(extension);
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
		getPayload(id, "multi", resultHandler);
	}

	private void payloadContent(Long id, String extension, Handler<AsyncResult<ReturnObject>> resultHandler) {
		if(extension.equals("json")) {
			getPayload(id, "json", resultHandler);
		}
		if(extension.equals("txt")) {
			getPayload(id, "txt", resultHandler);
		}
		getPayload(id, "multi", resultHandler);
	}

	private void getPayload(Long id, String extension, Handler<AsyncResult<ReturnObject>> resultHandler) {

		dataAssetManager.findById(id, reply -> {
			if (reply.succeeded()) {

				//TODO Get file Content from ADAPTER

//				fileContentFuture.setHandler( br -> {
//					if(br.succeeded()) {
//						if(extension.equals("json")) {
//							getJSON(br.result(), resultHandler);
//						} else {
//							getMultiPart(br.result(), extension, resultHandler);
//						}
//					}
//					else {
//						LOGGER.error("FileContent could not be read.\n\n"+br.cause());
//						resultHandler.handle(Future.failedFuture(""));
//					}
//				});
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

	private void getMultiPart(String fileContent, String extension, Handler<AsyncResult<ReturnObject>> resultHandler) {
		ContentBody	cb = new StringBody(Json.encode(idsService.getSelfDescriptionResponse()), ContentType.create("application/json"));
		MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
				.setBoundary("IDSMSGPART")
				.setCharset(StandardCharsets.UTF_8)
				.addPart("header", cb)
				.addTextBody("payload", fileContent, ContentType.create("text/plain", StandardCharsets.UTF_8));

		if(extension.equals("multi")) {
			multipartEntityBuilder.setContentType(ContentType.create("multipart/mixed"));
		}
		if(extension.equals("txt")) {
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

	private ContentTypeWrapper getContentTypeWrapper(String extension) {
		if(extension.equals(".ttl")) {
			return new ContentTypeWrapper("text/turtle", "TTL");
		}
		if(extension.equals(".jsonld")) {
			return new ContentTypeWrapper("application/ld+json", "JSON-LD");
		}
		if(extension.equals(".rdf")) {
			return new ContentTypeWrapper("application/rdf+xml", "RDF/XML");
		}
		return new ContentTypeWrapper("text/plain; charset=utf-8", "TTL");
	}
}