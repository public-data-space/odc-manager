package de.fraunhofer.fokus.ids.controllers;

import com.fasterxml.jackson.annotation.JsonInclude;
import de.fraunhofer.fokus.ids.enums.FileType;
import de.fraunhofer.fokus.ids.messages.ResourceRequest;
import de.fraunhofer.fokus.ids.models.*;
import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.entities.serialization.DataSourceSerializer;
import de.fraunhofer.fokus.ids.persistence.managers.DataAssetManager;
import de.fraunhofer.fokus.ids.persistence.managers.DataSourceManager;
import de.fraunhofer.fokus.ids.services.ConfigService;
import de.fraunhofer.fokus.ids.services.IDSService;
import de.fraunhofer.fokus.ids.services.datasourceAdapter.DataSourceAdapterService;
import de.fraunhofer.fokus.ids.utils.IDSMessageParser;
import de.fraunhofer.fokus.ids.utils.services.authService.AuthAdapterService;
import de.fraunhofer.iais.eis.*;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.HttpEntity;

import java.io.*;
import java.net.URI;
import java.text.ParseException;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class ConnectorController {

	private Logger LOGGER = LoggerFactory.getLogger(ConnectorController.class.getName());
	private IDSService idsService;
	private DataAssetManager dataAssetManager;
	private DataSourceManager dataSourceManager;
	private DataSourceAdapterService dataSourceAdapterService;
    private FileUploadController fileUploadController;
    private AuthAdapterService authAdapterService;
    private ConfigService configService;

	public ConnectorController(Vertx vertx){
		this.idsService = new IDSService(vertx);
		this.authAdapterService = AuthAdapterService.createProxy(vertx, Constants.AUTHADAPTER_SERVICE);
		this.dataAssetManager = new DataAssetManager();
		this.dataSourceManager = new DataSourceManager();
		this.fileUploadController = new FileUploadController(vertx);
		this.dataSourceAdapterService = DataSourceAdapterService.createProxy(vertx, Constants.DATASOURCEADAPTER_SERVICE);
		this.configService = new ConfigService(vertx);
		DatabindCodec.prettyMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
	}

	public void data(Message header, String extension, Handler<AsyncResult<HttpEntity>> resultHandler){
		URI uri = ((ArtifactRequestMessage) header).getRequestedArtifact();
		String path = uri.getPath();
		String idStr = path.substring(path.lastIndexOf('/') + 1);
		long id = Long.parseLong(idStr);

		Promise<ArtifactResponseMessage> artifactResponsePromise = Promise.promise();
		Future<ArtifactResponseMessage> artifactResponseFuture = artifactResponsePromise.future();
		configService.getConfigurationWithDAT(reply -> {
			if(reply.succeeded()) {
				idsService.getArtifactResponse(reply.result(),header.getId(), artifactResponseFuture);
			} else {
				LOGGER.info(reply.cause());
				artifactResponsePromise.fail(reply.cause());
			}
		});
		Promise<File> filePromise = Promise.promise();
		Future<File> fileFuture = filePromise.future();
		payload(id, extension, fileFuture);
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
		Message header = IDSMessageParser.parse(input).get().getHeader().get();
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
			routeMessage(header, resultHandler);
		} else {
			idsService.handleRejectionMessage(header.getId(),RejectionReason.MALFORMED_MESSAGE,resultHandler);
		}
	}

	private void routeMessage(Message header, Handler<AsyncResult<HttpEntity>> resultHandler){
			if (header == null) {
				idsService.handleRejectionMessage(header.getId(),RejectionReason.MALFORMED_MESSAGE,resultHandler);
			} else {
				authAdapterService.isAuthenticated(header.getSecurityToken().getTokenValue(), authreply -> {
					if(authreply.succeeded()) {
						if (header instanceof DescriptionRequestMessage) {
							multiPartAbout(header, resultHandler);
						} else if (header instanceof ArtifactRequestMessage) {
							data(header, "", resultHandler);
						} else {
							idsService.handleRejectionMessage(header.getId(), RejectionReason.MESSAGE_TYPE_NOT_SUPPORTED, resultHandler);
						}
					} else {
						idsService.handleRejectionMessage(header.getId(), RejectionReason.NOT_AUTHENTICATED, resultHandler);
					}
				});
			}
	}

	public void routeMessage(String input, Handler<AsyncResult<HttpEntity>> resultHandler) {
		routeMessage(IDSMessageParser.parse(input).get().getHeader().get(), resultHandler);
	}

	public void multiPartAbout(Message header, Handler<AsyncResult<HttpEntity>> resultHandler) {
		Promise<Connector> connectorPromise = Promise.promise();
		Future<Connector> connectorFuture = connectorPromise.future();
		Promise<DescriptionResponseMessage> responsePromise = Promise.promise();
		Future<DescriptionResponseMessage> responseFuture = responsePromise.future();

		configService.getConfigurationWithDAT(reply -> {
			if(reply.succeeded()) {
				idsService.getConnector(reply.result(), connectorFuture);
				idsService.getSelfDescriptionResponse(reply.result(), header.getId(), responseFuture);
			} else {
				LOGGER.info(reply.cause());
				connectorPromise.fail(reply.cause());
				responsePromise.fail(reply.cause());
			}
		});
		idsService.messageHandling(header.getId(), responseFuture, connectorFuture, resultHandler);
	}

	public void about(Handler<AsyncResult<String>> resultHandler) {
		configService.getConfigurationWithDAT(configReply -> {
			if(configReply.succeeded()) {
				idsService.getConnector(configReply.result(), reply -> {
					if (reply.succeeded()) {
						resultHandler.handle(Future.succeededFuture(Json.encodePrettily(reply.result())));
					} else {
						LOGGER.error("Connector Object could not be retrieved.", reply.cause());
						resultHandler.handle(Future.failedFuture(reply.cause()));
					}
				});
			} else {

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
		dataSourceManager.findByType("File Upload",jsonObjectAsyncResult -> {
			if (jsonObjectAsyncResult.succeeded()) {
				try {
					DataSource dataSourceFileUpload = DataSourceSerializer.deserialize(jsonObjectAsyncResult.result().getJsonObject(0));
					dataAssetManager.findById(id, reply -> {
						if (reply.succeeded()) {
							DataAsset dataAsset = Json.decodeValue(reply.result().toString(), DataAsset.class);
							if (dataAsset.getSourceID().equals(dataSourceFileUpload.getId())) {
								fileUploadController.getFileUpload(resultHandler, dataAsset);
							} else {
								dataSourceManager.findById(dataAsset.getSourceID(), reply2 -> {
									if (reply2.succeeded()) {
										DataSource dataSource = null;
										try {
											dataSource = DataSourceSerializer.deserialize(reply2.result());
										} catch (ParseException e) {
											LOGGER.error(e);
											resultHandler.handle(Future.failedFuture(e));
										}

										ResourceRequest request = new ResourceRequest();
										request.setDataSource(dataSource);
										request.setDataAsset(dataAsset);
										request.setFileType(fileType);

										dataSourceAdapterService.getFile(dataSource.getDatasourceType(), new JsonObject(Json.encode(request)), reply3 -> {
											if (reply3.succeeded()) {
												resultHandler.handle(Future.succeededFuture(new File(reply3.result())));
											} else {
												LOGGER.error("FileContent could not be retrieved.", reply3.cause());
												resultHandler.handle(Future.failedFuture(reply3.cause()));
											}
										});
									} else {
										LOGGER.error("DataAsset could not be retrieved.", reply2.cause());
										resultHandler.handle(Future.failedFuture(reply2.cause()));
									}
								});
							}
						} else {
							LOGGER.error(reply.cause());
							resultHandler.handle(Future.failedFuture(reply.cause()));
						}
					});
				} catch (ParseException e) {
					LOGGER.error(e);
					resultHandler.handle(Future.failedFuture(e));
				}
			} else {
			LOGGER.error(jsonObjectAsyncResult.cause());
			resultHandler.handle(Future.failedFuture(jsonObjectAsyncResult.cause()));
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