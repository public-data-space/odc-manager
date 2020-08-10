package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import de.fraunhofer.fokus.ids.persistence.managers.DataAssetManager;
import de.fraunhofer.iais.eis.*;
import de.fraunhofer.iais.eis.ids.jsonld.Serializer;
import de.fraunhofer.iais.eis.util.TypedLiteral;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.StringBody;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.List;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class IDSService {
	private final Logger LOGGER = LoggerFactory.getLogger(IDSService.class.getName());

	private String INFO_MODEL_VERSION = "3.1.0";
	private String[] SUPPORTED_INFO_MODEL_VERSIONS = {"3.1.0"};
	private DataAssetManager dataAssetManager;
	private ConfigService configService;
    private Serializer serializer = new Serializer();
	public IDSService(Vertx vertx){
		dataAssetManager = new DataAssetManager();
		configService = new ConfigService(vertx);
	}

	public void getSelfDescriptionResponse(JsonObject config, URI uri, Handler<AsyncResult<Message>> resultHandler) {
		resultHandler.handle(Future.succeededFuture( buildSelfDescriptionResponse(uri, config)));
	}

	public void getArtifactResponse(JsonObject config, URI uri, Handler<AsyncResult<Message>> resultHandler) {
		resultHandler.handle(Future.succeededFuture( buildArtifactResponseMessage(uri, config)));
	}

	private ArtifactResponseMessage buildArtifactResponseMessage(URI uri, JsonObject config) {
		try {
			return new ArtifactResponseMessageBuilder(new URI(config.getString("url") + "/ArtifactResponseMessage/" +UUID.randomUUID().toString()))
					._issued_(getDate())
					._correlationMessage_(uri)
					._modelVersion_(INFO_MODEL_VERSION)
					._issuerConnector_(new URI(config.getString("url")+"#Connector"))
					._securityToken_( new DynamicAttributeTokenBuilder()
							._tokenFormat_(TokenFormat.JWT)
							._tokenValue_(config.getString("jwt"))
							.build())
					.build();
		} catch (URISyntaxException e) {
			LOGGER.error(e);
		}
		return null;
	}

	private XMLGregorianCalendar getDate(){
		GregorianCalendar c = new GregorianCalendar();
		c.setTime(new Date());
		try {
			return DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
		} catch (DatatypeConfigurationException e) {
			LOGGER.error(e);
		}
		return null;
	}

	public void createRegistrationMessage(JsonObject config, Handler<AsyncResult<Message>> resultHandler){
				try {
					ConnectorAvailableMessage message = new ConnectorAvailableMessageBuilder()
							._issued_(getDate())
							._modelVersion_(INFO_MODEL_VERSION)
							._issuerConnector_(new URI(config.getString("url")+"#Connector"))
							._securityToken_( new DynamicAttributeTokenBuilder()
									._tokenFormat_(TokenFormat.JWT)
									._tokenValue_(config.getString("jwt"))
									.build())
							.build();
					resultHandler.handle(Future.succeededFuture(message));
				} catch (URISyntaxException e) {
					LOGGER.error(e);
					resultHandler.handle(Future.failedFuture(e));
				}
	}

	public void createUpdateMessage(JsonObject config, Handler<AsyncResult<Message>> resultHandler){
				try {
					ConnectorUpdateMessage message = new ConnectorUpdateMessageBuilder()
							._issued_(getDate())
							._modelVersion_(INFO_MODEL_VERSION)
							._issuerConnector_(new URI(config.getString("url")+"#Connector"))
							._securityToken_( new DynamicAttributeTokenBuilder()
									._tokenFormat_(TokenFormat.JWT)
									._tokenValue_(config.getString("jwt"))
									.build())
							.build();
					resultHandler.handle(Future.succeededFuture(message));
				} catch (URISyntaxException e) {
					LOGGER.error(e);
					resultHandler.handle(Future.failedFuture(e));
				}
	}

	public void createUnregistrationMessage(JsonObject config, Handler<AsyncResult<Message>> resultHandler){
				try {
					ConnectorUnavailableMessage message = new ConnectorUnavailableMessageBuilder()
							._issued_(getDate())
							._modelVersion_(INFO_MODEL_VERSION)
							._issuerConnector_(new URI(config.getString("url")+"#Connector"))
							._securityToken_( new DynamicAttributeTokenBuilder()
									._tokenFormat_(TokenFormat.JWT)
									._tokenValue_(config.getString("jwt"))
									.build())
							.build();
					resultHandler.handle(Future.succeededFuture(message));
				} catch (URISyntaxException e) {
					LOGGER.error(e);
					resultHandler.handle(Future.failedFuture(e));
				}
	}

	public void getConnector(JsonObject config, Handler<AsyncResult<Connector>> resultHandler) {
				buildBaseConnector(config, reply -> {
					if (reply.succeeded()) {
						resultHandler.handle(Future.succeededFuture(reply.result()));
					} else {
						LOGGER.error(reply.cause());
						resultHandler.handle(Future.failedFuture(reply.cause()));
					}
				});
	}

	private DescriptionResponseMessage buildSelfDescriptionResponse(URI uri, JsonObject config){

		try {
			return new DescriptionResponseMessageBuilder(new URI(config.getString("url") +"/DescriptionResponseMessage/"+UUID.randomUUID()))
					._issued_(getDate())
					._correlationMessage_(uri)
					._modelVersion_(INFO_MODEL_VERSION)
					._issuerConnector_(new URI(config.getString("url")+"#Connector"))
					._securityToken_( new DynamicAttributeTokenBuilder()
							._tokenFormat_(TokenFormat.JWT)
							._tokenValue_(config.getString("jwt"))
							.build())
					.build();
		} catch (URISyntaxException e) {
			LOGGER.error(e);
		}
		return null;
	}

	private void buildBaseConnector(JsonObject config, Handler<AsyncResult<Connector>> next){

		Future<Catalog> catalogFuture = buildCatalog(config);
		try {
			BaseConnectorBuilder connectorBuilder = new BaseConnectorBuilder((new URI(config.getString("url") + "#Connector")))
					._maintainer_(new URI(config.getString("maintainer")))
					._version_("0.0.1")
					._curator_(new URI(config.getString("curator")))
					._physicalLocation_(new GeoFeatureBuilder(new URI(config.getString("country"))).build())
					._outboundModelVersion_(INFO_MODEL_VERSION)
					._inboundModelVersion_(new ArrayList<>(Arrays.asList(SUPPORTED_INFO_MODEL_VERSIONS)))

					//TODO Change dummy auth service (can be null)
					//				._authInfo_(new AuthInfoBuilder(new URL(this.connectorURL + "#AuthInfo"))
					//						._authService_(new URI(this.connectorURL + "#AuthService"))
					//						._authStandard_(AuthStandard.OAUTH2_JWT)
					//						.build())

					._securityProfile_(SecurityProfile.BASE_CONNECTOR_SECURITY_PROFILE)
					._title_(new ArrayList<>(Arrays.asList(new TypedLiteral(config.getString("title")))))
					._host_(new ArrayList<>(Arrays.asList(new HostBuilder()
							._accessUrl_(new URI(config.getString("url")))
							._protocol_(Protocol.HTTP)
							.build())));

					//TODO fill with information
					//				._descriptions_(new ArrayList<PlainLiteral>(Arrays.asList(new PlainLiteral(""))))
					//				._lifecycleActivities_(null)
					//				._componentCertification_(null)
					//				._physicalLocation_(null);

			catalogFuture.onComplete( ac -> {
				if(ac.succeeded()) {
					connectorBuilder._catalog_(catalogFuture.result());
					next.handle(Future.succeededFuture(connectorBuilder.build()));
				}
				else{
					LOGGER.error(ac.cause());
					next.handle(Future.failedFuture(ac.cause()));
				}
			});

		} catch (Exception e) {
			LOGGER.error(e);
			next.handle(Future.failedFuture(e.getMessage()));
		}
	}

	private Future<Catalog> buildCatalog(JsonObject config) {

		Future<List<Resource>> offers = getOfferResources(config);
		Future<List<Resource>> requests = getRequestResources(config);
		Promise<Catalog> catalog = Promise.promise();

		CompositeFuture.all(offers, requests).onComplete(cf -> {
			if(cf.succeeded()) {
				try {
					catalog.complete(new CatalogBuilder(new URI(config.getString("url") + "#Catalog"))
							._offer_(new ArrayList(offers.result()))
							._request_(new ArrayList(requests.result()))
							.build());
				} catch (Exception e) {
					LOGGER.error(e);
					catalog.fail(e.getMessage());
				}
			}
			else{
				LOGGER.error(cf.cause());
				catalog.fail(cf.cause());
			}
		});
		return catalog.future();
	}

	private Future<List<Resource>> getRequestResources(JsonObject config) {
		// TODO Auto-generated method stub
		return Future.succeededFuture(new ArrayList<>());
	}

	private Future<List<Resource>> getOfferResources(JsonObject config) {
		Promise<List<Resource>> daPromise = Promise.promise();
		findPublished(daList -> createDataResources(config, daList, daPromise));
		return daPromise.future();
	}

	private void createDataResources( JsonObject config, AsyncResult<List<DataAsset>> daList, Promise<List<Resource>> daPromise) {
		if(daList.succeeded()) {
			List<DataAsset> das = daList.result();
			ArrayList<Resource> offerResources = new ArrayList<>();
			for (DataAsset da : das) {
				try {
					DataResourceBuilder r = new DataResourceBuilder(new URI(config.getString("url") + "/DataResource/"+da.getId()))
							//						//TODO: The regular period with which items are added to a collection.
							//						._accrualPeriodicity_(null)
							//						//TODO: Reference to a Digital Content (physically or logically) included, definition of part-whole hierarchies.
							//						._contentParts_(null)
							//						//TODO: Constraint that refines a (composite) Digital Content.
							//						._contentRefinements_(null)
							//						//TODO: Standards document defining the given Digital Content. The content is assumed to conform to that Standard.
							//						._contentStandard_(null)
							//						//TODO: Enumerated types of content expanding upon the Digital Content hierarchy.
							//						._contentType_(null)
							//						//TODO: Reference to a Contract Offer defining the authorized use of the Resource.
							//						._contractOffers_(null)
							//						//TODO: Default representation of the content.
							//						._defaultRepresentation_(null)
							//						//TODO: Natural language(s) used within the content
							//						._languages_(null)
							//						//TODO: Something that occurs over a period of time and acts upon or with entities.
							//						._lifecycleActivities_(null)
							//						//TODO: Representation of the content.
							//						._representations_(null)
							//						//TODO: Reference to the Interface defining Operations supported by the Resource.
							//						._resourceInterface_(null)
							//						//TODO: Reference to a Resource (physically or logically) included, definition of part-whole hierarchies.
							//						._resourceParts_(null)
							//						//TODO: Sample Resource instance.
							//						._samples_(null)
							//						//TODO: Named spatial entity covered by the Resource.
							//						._spatialCoverages_(null)
							//						//TODO: Reference to a well-known License regulating the general usage of the Resource.
							//						._standardLicense_(null)
							//						//TODO: Temporal period or instance covered by the content.
							//						._temporalCoverages_(null)
							//						//TODO: Abstract or concrete concept related to or referred by the content.
							//						._themes_(null)
							//						//TODO: (Equivalent) variant of given Resource, e.g. a translation.
							//						._variant_(null)
							//._publisher_(null)
							//._sovereign_(null);

							._version_(da.getVersion())
							._resourceEndpoint_(getResourceEndpoint(config, da));

					if (da.getDatasetTitle() != null) {
						r._title_(new ArrayList<>(Arrays.asList(new TypedLiteral(da.getDatasetTitle()))));
					}
					if (da.getDataSetDescription() != null) {
						r._description_(new ArrayList<>(Arrays.asList(new TypedLiteral(da.getDataSetDescription()))));
					}
					ArrayList<TypedLiteral> keywords = getKeyWords(da);
					if (keywords != null) {
						r._keyword_(getKeyWords(da));
					}
					if (da.getLicenseUrl() != null) {
						r._customLicense_(new URI(da.getLicenseUrl()));
					}

					offerResources.add(r.build());
				} catch (Exception e) {
					LOGGER.error( e);
				}
			}
			daPromise.complete(offerResources);
		}
	}

	private void findPublished(Handler<AsyncResult<List<DataAsset>>> next) {

		dataAssetManager.findPublished(reply -> {
			if(reply.succeeded()) {

				JsonArray array = new JsonArray(reply.result().toString());
				List<DataAsset> assets = new ArrayList<>();
				for(int i=0;i<array.size();i++){
					assets.add(Json.decodeValue(array.getJsonObject(i).toString(), DataAsset.class));
				}
				next.handle(Future.succeededFuture(assets));
			}
			else{
				LOGGER.error(reply.cause());
				next.handle(Future.succeededFuture(new ArrayList<>()));
			}
		});
	}

	private ArrayList<? extends Endpoint> getResourceEndpoint(JsonObject config, DataAsset da) {
		ArrayList<Endpoint> endpoints = new ArrayList<>();
		Endpoint e;
		try {
			e = new StaticEndpointBuilder(new URI(config.getString("url")+"/ResourceEndpoint/"+UUID.randomUUID()))
					._endpointArtifact_(new ArtifactBuilder(new URI(config.getString("url")+"/Artifact/"+da.getId()))
							._creationDate_(getDate(da.getCreatedAt()))
							._fileName_(da.getId().toString())
							.build())
					._endpointHost_(new HostBuilder(new URI(config.getString("url")+"#Host"))
							._accessUrl_(new URI(config.getString("url")))
							._pathPrefix_("/")
							._protocol_(Protocol.HTTP)
							.build())
					._path_("/data/")
					.build();
			endpoints.add(e);
		} catch (Exception e1) {
			LOGGER.error(e1);
		}
		return endpoints;
	}

	private XMLGregorianCalendar getDate(java.time.Instant createdAt) {

		GregorianCalendar cal1 = new GregorianCalendar();
		cal1.setTimeInMillis(createdAt.toEpochMilli());

		try {
			return DatatypeFactory.newInstance().newXMLGregorianCalendar(cal1);
		} catch (DatatypeConfigurationException e) {
			LOGGER.error(e);
		}
		return null;
	}

	private ArrayList<TypedLiteral> getKeyWords(DataAsset da) {
		ArrayList<TypedLiteral> keywords = new ArrayList<>();
		for (String tag : da.getTags()) {
			keywords.add(new TypedLiteral(tag));
		}
		return keywords.isEmpty()? null : keywords;
	}

	private void createRejectionMessage(JsonObject config, URI uri,RejectionReason rejectionReason,Handler<AsyncResult<RejectionMessage>> resultHandler) {
				try {
					RejectionMessage message = new RejectionMessageBuilder(new URI(config.getString("url") + "/RejectionMessage/" + UUID.randomUUID()))
							._correlationMessage_(uri)
							._issued_(getDate())
							._modelVersion_(INFO_MODEL_VERSION)
							._issuerConnector_(new URI(config.getString("url")+"#Connector"))
							._securityToken_(new DynamicAttributeTokenBuilder()
									._tokenFormat_(TokenFormat.JWT)
									._tokenValue_(config.getString("jwt"))
									.build())
							._rejectionReason_(rejectionReason)
							.build();
					resultHandler.handle(Future.succeededFuture(message));
				} catch (URISyntaxException e) {
					LOGGER.error(e);
					resultHandler.handle(Future.failedFuture(e));
				}
	}

	public void handleDataMessage(URI uri, Future<Message> header, Future payload, long assetId, Handler<AsyncResult<HttpEntity>> resultHandler) {
		CompositeFuture.all(header,payload).onComplete( reply -> {
			if(reply.succeeded()) {
				getFileName(assetId, fileNameReply -> {
					if(fileNameReply.succeeded()) {
						MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
								.setBoundary("msgpart")
								.addPart("header", new StringBody(Json.encodePrettily(header.result()), org.apache.http.entity.ContentType.create("application/json")))
								.addBinaryBody("payload", (File) payload.result(), ContentType.create("application/octet-stream"), fileNameReply.result());
						resultHandler.handle(Future.succeededFuture(multipartEntityBuilder.build()));
						((File) payload.result()).delete();
					} else {
						MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
								.setBoundary("msgpart")
								.addPart("header", new StringBody(Json.encodePrettily(header.result()), org.apache.http.entity.ContentType.create("application/json")))
								.addBinaryBody("payload", (File) payload.result(), ContentType.create("application/octet-stream"), UUID.randomUUID().toString());
						resultHandler.handle(Future.succeededFuture(multipartEntityBuilder.build()));
						((File) payload.result()).delete();
					}
				});
			}else{
				handleRejectionMessage(uri,RejectionReason.INTERNAL_RECIPIENT_ERROR,resultHandler);
				LOGGER.error(reply.cause());
				}
			});
	}

	public void handleAbouMessage(URI uri, Future<Message> header, Future<Connector> payload, Handler<AsyncResult<HttpEntity>> resultHandler) {
		CompositeFuture.all(header, payload).onComplete( reply -> {
			if (reply.succeeded()) {
				MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
						.setBoundary("msgpart")
						.setCharset(StandardCharsets.UTF_8)
						.setContentType(ContentType.APPLICATION_JSON)
						.addPart("header", new StringBody(Json.encodePrettily(header.result()), org.apache.http.entity.ContentType.create("application/json")))
						.addPart("payload", new StringBody(Json.encodePrettily(payload.result()), org.apache.http.entity.ContentType.create("application/json")));
				resultHandler.handle(Future.succeededFuture(multipartEntityBuilder.build()));

			} else {
				handleRejectionMessage(uri, RejectionReason.INTERNAL_RECIPIENT_ERROR, resultHandler);
				LOGGER.error(reply.cause());
			}
		});
	}

	public void getFileName(Long id, Handler<AsyncResult<String>> result){
		dataAssetManager.findById(id,jsonObjectAsyncResult -> {
			if (jsonObjectAsyncResult.succeeded()){
				DataAsset dataAsset = Json.decodeValue(jsonObjectAsyncResult.result().toString(), DataAsset.class);
				result.handle(Future.succeededFuture(dataAsset.getFilename()));
			}
			else {
				LOGGER.error(jsonObjectAsyncResult.cause());
				result.handle(Future.failedFuture(jsonObjectAsyncResult.cause()));
			}
		});
	}

	public void handleRejectionMessage(URI uri,RejectionReason rejectionReason,Handler<AsyncResult<HttpEntity>> resultHandler) {
		configService.getConfigurationWithDAT(configReply -> {
			if(configReply.succeeded()) {
				createRejectionMessage(configReply.result(),uri, rejectionReason, rejectionMessageAsyncResult -> {
					if (rejectionMessageAsyncResult.succeeded()) {
						HttpEntity reject = createMultipartMessage(rejectionMessageAsyncResult.result());
						resultHandler.handle(Future.succeededFuture(reject));
					} else {
						resultHandler.handle(Future.failedFuture(rejectionMessageAsyncResult.cause()));
					}
				});
			} else {
				LOGGER.error(configReply.cause());
				resultHandler.handle(Future.failedFuture(configReply.cause()));
			}
		});
	}

	private HttpEntity createMultipartMessage(Message message) {
		ContentBody cb = null;
		try {
			cb = new StringBody(serializer.serialize(message), ContentType.create("application/json"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
				.setBoundary("msgpart")
				.setCharset(StandardCharsets.UTF_8)
				.setContentType(ContentType.APPLICATION_JSON)
				.addPart("header", cb);

			return multipartEntityBuilder.build();
	}

}
