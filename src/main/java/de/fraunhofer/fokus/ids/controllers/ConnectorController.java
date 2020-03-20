package de.fraunhofer.fokus.ids.controllers;

import com.fasterxml.jackson.annotation.JsonInclude;
import de.fraunhofer.fokus.ids.enums.FileType;
import de.fraunhofer.fokus.ids.messages.ResourceRequest;
import de.fraunhofer.fokus.ids.models.*;
import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.managers.DataAssetManager;
import de.fraunhofer.fokus.ids.persistence.managers.DataSourceManager;
import de.fraunhofer.fokus.ids.services.IDSMessageParser;
import de.fraunhofer.fokus.ids.services.IDSService;
import de.fraunhofer.fokus.ids.services.datasourceAdapter.DataSourceAdapterService;
import de.fraunhofer.iais.eis.*;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.HttpEntity;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
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

	public void data(Message header, String extension, Handler<AsyncResult<HttpEntity>> resultHandler){
		URI uri = ((ArtifactRequestMessage) header).getRequestedArtifact();
		String path = uri.getPath();
		String idStr = path.substring(path.lastIndexOf('/') + 1);
		long id = Long.parseLong(idStr);

		Future<ArtifactResponseMessage> artifactResponseFuture = Future.future();
		idsService.getArtifactResponse(artifactResponseFuture.completer());
		Future<File> fileFuture = Future.future();
		payload(id, extension, fileFuture.completer());
		idsService.messageHandling(header.getId(),artifactResponseFuture,fileFuture,resultHandler);
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

	private Message getHeader(String input,Handler<AsyncResult<HttpEntity>> resultHandler){
		JsonObject jsonObject = IDSMessageParser.getHeader(input);
		Message header = Json.decodeValue(jsonObject.toString(),Message.class);
		if (header == null) {
			try {
				idsService.handleRejectionMessage(new URI(String.valueOf(RejectionReason.MALFORMED_MESSAGE)), RejectionReason.MALFORMED_MESSAGE, resultHandler);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return header;
	}

	public void checkMessage(String input, Class clazz, Handler<AsyncResult<HttpEntity>> resultHandler) {
		Message header = getHeader(input, resultHandler);
		if (clazz.isInstance(header)) {
			routeMessage(input, header, resultHandler);
		} else {
			idsService.handleRejectionMessage(header.getId(),RejectionReason.MALFORMED_MESSAGE,resultHandler);
		}
	}

	private void routeMessage(String input, Message header, Handler<AsyncResult<HttpEntity>> resultHandler){
			if (header == null) {
				idsService.handleRejectionMessage(header.getId(),RejectionReason.MALFORMED_MESSAGE,resultHandler);
			} else {
				if (header instanceof SelfDescriptionRequest) {
					LOGGER.info("SelfDescriptionRequest received.");
					multiPartAbout(header, resultHandler);
				} else if (header instanceof ArtifactRequestMessage) {
					LOGGER.info("ArtifactRequestMessage received.");
					data( header, "", resultHandler);
				} else {
					idsService.handleRejectionMessage(header.getId(),RejectionReason.MESSAGE_TYPE_NOT_SUPPORTED,resultHandler);
				}
			}
	}

	public void routeMessage(String input, Handler<AsyncResult<HttpEntity>> resultHandler) {
		routeMessage(input, IDSMessageParser.getHeader(input), resultHandler);
	}

	public void multiPartAbout(Message header, Handler<AsyncResult<HttpEntity>> resultHandler) {
		Future<Connector> connectorFuture = Future.future();
		idsService.getConnector(connectorFuture.completer());
		Future<SelfDescriptionResponse> responseFuture = Future.future();
		idsService.getSelfDescriptionResponse(responseFuture.completer());
		idsService.messageHandling(header.getId(), responseFuture, connectorFuture, resultHandler);
	}

	public void about(Handler<AsyncResult<String>> resultHandler) {

		idsService.getConnector( reply -> {
			if (reply.succeeded()) {
				resultHandler.handle(Future.succeededFuture(Json.encodePrettily(reply.result())));
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
				LOGGER.error(reply.cause());
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