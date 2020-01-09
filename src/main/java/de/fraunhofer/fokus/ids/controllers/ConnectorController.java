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
import de.fraunhofer.iais.eis.Connector;
import de.fraunhofer.iais.eis.SelfDescriptionResponse;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.StringBody;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

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

	public void data(long id, String extension, Handler<AsyncResult<File>> resultHandler) {
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
	private void addToZipFile(List<String> myList, ZipOutputStream zipOut) throws IOException {
		for (String filePath : myList){
			File input = new File(filePath.trim());
			FileInputStream fis = new FileInputStream(input);
			ZipEntry ze = new ZipEntry(input.getName());
			zipOut.putNextEntry(ze);
			byte[] tmp = new byte[4*1024];
			int size;
			while((size = fis.read(tmp)) != -1){
				zipOut.write(tmp, 0, size);
			}
			zipOut.flush();
			fis.close();
		}
		zipOut.close();

	}
	private void getPayload(Long id, FileType fileType, Handler<AsyncResult<File>> resultHandler) {
		dataSourceManager.findByType("File Upload",jsonObjectAsyncResult -> {
			if (jsonObjectAsyncResult.succeeded()){
				String dts = jsonObjectAsyncResult.result().toString().replace("[","").replace("]","");
				DataSource dataSourceFileUpload = Json.decodeValue(dts, DataSource.class);
				dataAssetManager.findById(id, reply -> {
					if (reply.succeeded()) {
						DataAsset dataAsset = Json.decodeValue(reply.result().toString(), DataAsset.class);
						if (dataAsset.getSourceID().equals(dataSourceFileUpload.getId())) {
							String getFiles = dataAsset.getUrl();
							String replace = getFiles.replace("[","");
							String replace1 = replace.replace("]","");
							List<String> myList = Arrays.asList(replace1.split(","));
							if (myList.size()>1){
								String tempName = "test.zip";
								File file = new File(tempName);
								FileOutputStream fos ;
								ZipOutputStream zipOut ;
								try {
									fos = new FileOutputStream(tempName);
									zipOut = new ZipOutputStream(new BufferedOutputStream(fos));
									addToZipFile(myList,zipOut);
								} catch (IOException e) {
									e.printStackTrace();
								}

								resultHandler.handle(Future.succeededFuture(file));
							}else {
								File file = new File(myList.get(0));
								resultHandler.handle(Future.succeededFuture(file));
							}
						}
						else {
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

					}
					else {
						LOGGER.error("DataAsset could not be read.",reply.cause());
						resultHandler.handle(Future.failedFuture(reply.cause()));
					}
				});
			}
			else {
				LOGGER.error("Error ",jsonObjectAsyncResult.cause());
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