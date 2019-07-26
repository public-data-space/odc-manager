package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import de.fraunhofer.fokus.ids.persistence.managers.DataAssetManager;
import de.fraunhofer.iais.eis.*;
import de.fraunhofer.iais.eis.util.ConstraintViolationException;
import de.fraunhofer.iais.eis.util.PlainLiteral;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;

public class IDSService {
	final Logger LOGGER = LoggerFactory.getLogger(IDSService.class.getName());

	private String INFO_MODEL_VERSION = "1.0.2";
	private String[] SUPPORTED_INFO_MODEL_VERSIONS = {"1.0.0.","1.0.1","1.0.2"};
	private String baseURL;
	private String connectorURL;
	private DataAssetManager dataAssetManager;

	public IDSService(Vertx vertx){
		dataAssetManager = new DataAssetManager(vertx);
	}

	private String getPayloadURL(Long id) {
		//TODO FIX THIS
		return "http://127.0.0.1:8080/payload/";
	}

	public SelfDescriptionResponse getSelfDescriptionResponse() {
		//TODO FIX THIS
		this.connectorURL = "http://127.0.0.1:8080";
		try {
			return buildSelfDescriptionResponse();
		} catch (MalformedURLException e) {
			LOGGER.error("Exception:",e);
		} catch (ConstraintViolationException e) {
			LOGGER.error("Exception:",e);
		} catch (DatatypeConfigurationException e) {
			LOGGER.error("Exception:",e);
		}
		return null;

	}

	public void getConnector(Handler<AsyncResult<Connector>> resultHandler) {
		this.baseURL = "http://127.0.0.1:8080";
		this.connectorURL = "http://127.0.0.1:8080";

		buildBaseConnector(reply -> {
			if(reply.succeeded()){
				resultHandler.handle(Future.succeededFuture(reply.result()));
			}
			else{
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	private SelfDescriptionResponse buildSelfDescriptionResponse() throws MalformedURLException, ConstraintViolationException, DatatypeConfigurationException {
		GregorianCalendar c = new GregorianCalendar();
		c.setTime(new Date());
		XMLGregorianCalendar xmlDate = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);

		return new SelfDescriptionResponseBuilder(new URL(this.connectorURL +"#SelfDescriptionResponse"))
				._issued_(xmlDate)
				._issuerConnector_(new URL(this.connectorURL))
				._modelVersion_(INFO_MODEL_VERSION)
				.build();
	}



	private void buildBaseConnector(Handler<AsyncResult<Connector>> next){

		Future<Catalog> catalogFuture = buildCatalog();
		try {
			BaseConnectorBuilder connectorBuilder = new BaseConnectorBuilder((new URL(this.connectorURL)))
					._maintainer_(new URL("http://ids.fokus.fraunhofer.de"))
					._version_("0.0.1")
					//TODO Curator?
					._curator_(new URL("http://ids.fokus.fraunhofer.de"))
					._outboundModelVersion_(INFO_MODEL_VERSION)
					//TODO Consume versions?
					._inboundModelVersions_(new ArrayList<>(Arrays.asList(SUPPORTED_INFO_MODEL_VERSIONS)))
					//TODO Change dummy auth service (can be null)
					//				._authInfo_(new AuthInfoBuilder(new URL(this.connectorURL + "#AuthInfo"))
					//						._authService_(new URI(this.connectorURL + "#AuthService"))
					//						._authStandard_(AuthStandard.OAUTH2_JWT)
					//						.build())
					//TODO Check security profile
					._securityProfile_(new SecurityProfileBuilder(new URL(this.connectorURL + "#SecurityProfile"))
							._basedOn_(PredefinedSecurityProfile.BASECONNECTORSECURITYPROFILE)
							._authenticationSupport_(AuthenticationSupport.NO_AUTHENTICATION)
							.build())
					._titles_(new ArrayList<>(Arrays.asList(new PlainLiteral("IDS Connector for Open Data", "en"))))
					//TODO Change dummy host
					._hosts_(new ArrayList<>(Arrays.asList(new HostBuilder()
							._accessUrl_(new URI(this.connectorURL))
							._protocol_(Protocol.HTTP)
							.build())));

					//TODO fill with information
					//				._descriptions_(new ArrayList<PlainLiteral>(Arrays.asList(new PlainLiteral(""))))
					//				._lifecycleActivities_(null)
					//				._componentCertification_(null)
					//				._physicalLocation_(null);

			catalogFuture.setHandler( ac -> {
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
			LOGGER.error("", e);
			next.handle(Future.failedFuture(e.getMessage()));
		}
	}

	private Future<Catalog> buildCatalog() {

		Future<List<Resource>> offers = getOfferResources();
		Future<List<Resource>> requests = getRequestResources();
		Future<Catalog> catalog = Future.future();

		CompositeFuture.all(offers, requests).setHandler(cf -> {
			if(cf.succeeded()) {
				try {
					catalog.complete(new CatalogBuilder(new URL(this.connectorURL + "#Catalog"))
							._offers_(new ArrayList(offers.result()))
							._requests_(new ArrayList(requests.result()))
							.build());
				} catch (MalformedURLException e) {
					LOGGER.error("Exception:", e);
					catalog.fail(e.getMessage());
				} catch (ConstraintViolationException e) {
					LOGGER.error("Exception:", e);
					catalog.fail(e.getMessage());
				}
			}
			else{
				LOGGER.error(cf.cause());
				catalog.fail(cf.cause());
			}
		});
		return catalog;
	}

	private Future<List<Resource>> getRequestResources() {
		// TODO Auto-generated method stub
		return Future.succeededFuture(new ArrayList<>());
	}

	private Future<List<Resource>> getOfferResources() {
		Future<List<Resource>> daFuture = Future.future();
		findPublished(daList -> createDataResources(daList, daFuture));
		return daFuture;
	}

	private void createDataResources(AsyncResult<List<DataAsset>> daList, Future<List<Resource>> daFuture) {
		if(daList.succeeded()) {
			List<DataAsset> das = daList.result();
			ArrayList<Resource> offerResources = new ArrayList<Resource>();
			for (DataAsset da : das) {
				try {
					DataResourceBuilder r = new DataResourceBuilder(new URL(getPayloadURL(da.getId()) + "#DataResource"))
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

							._version_(da.getVersion())
							._resourceEndpoints_(getResourceEndpoint(da))
							._publisher_(getAgent(da.getId(), "Publisher", "", ""))
							._sovereign_(getAgent(da.getId(), "Sovereign", da.getOrganizationDescription(), da.getOrganizationTitle()));
					if (da.getDatasetTitle() != null) {
						r._titles_(new ArrayList<>(Arrays.asList(new PlainLiteral(da.getDatasetTitle()))));
					}
					if (da.getDataSetDescription() != null) {
						r._descriptions_(new ArrayList<>(Arrays.asList(new PlainLiteral(da.getDataSetDescription()))));
					}
					ArrayList<PlainLiteral> keywords = getKeyWords(da);
					if (keywords != null) {
						r._keywords_(getKeyWords(da));
					}
					if (da.getLicenseUrl() != null) {
						r._customLicense_(new URI(da.getLicenseUrl()));
					}

					offerResources.add(r.build());
				} catch (MalformedURLException e) {
					LOGGER.error("Exception:", e);
				} catch (ConstraintViolationException e) {
					LOGGER.error("Exception:", e);
				} catch (URISyntaxException e) {
					LOGGER.error("Exception:", e);
				}
			}
			daFuture.complete(offerResources);
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
				next.handle(Future.succeededFuture(new ArrayList<>()));
			}
		});
	}

	private ArrayList<? extends Endpoint> getResourceEndpoint(DataAsset da) {
		ArrayList<Endpoint> endpoints = new ArrayList<>();
		Endpoint e;
		try {
			e = new StaticEndpointBuilder(new URL(getPayloadURL(da.getId())+"#ResourceEndpoint"))
					._endpointArtifact_(new ArtifactBuilder(new URL(getPayloadURL(da.getId())+"#Artifact"))
							._creationDate_(getDate(da.getCreatedAt()))
							._fileName_(da.getAccessInformation())
							.build())
					._endpointHost_(new HostBuilder(new URL(getPayloadURL(da.getId())+"#Host"))
							._accessUrl_(new URI(getPayloadURL(da.getId())))
							._pathPrefix_("/")
							._protocol_(Protocol.HTTP)
							.build())
					._path_(getPayloadURL(da.getId()))
					.build();
			endpoints.add(e);
		} catch (MalformedURLException e1) {
			LOGGER.error("Exception:",e1);
		} catch (ConstraintViolationException e1) {
			LOGGER.error("Exception:",e1);
		} catch (URISyntaxException e1) {
			LOGGER.error("Exception:",e1);
		}
		return endpoints;
	}

	private XMLGregorianCalendar getDate(Date createdAt) {
		try {
			DateFormat format = new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss.SSSSSS");
			String date = format.format(createdAt);


			GregorianCalendar cal = new GregorianCalendar();
			cal.setTime(format.parse(date));

			return  DatatypeFactory.newInstance().newXMLGregorianCalendar(cal);

		} catch (ParseException e) {
			LOGGER.error("Exception:",e);
		} catch (DatatypeConfigurationException e) {
			LOGGER.error("Exception:",e);
		}
		return null;
	}

	private Agent getAgent(Long id, String agentRole, String agentDescription , String agentTitle) {
		Agent agent = null;
		PlainLiteral desc = agentDescription != null ? new PlainLiteral(agentDescription) : null;
		PlainLiteral title = agentTitle != null ? new PlainLiteral(agentTitle) : null;
		try {
			agent = new AgentBuilder(new URL(getPayloadURL(id)+"#"+agentRole))
					._descriptions_(new ArrayList<>(Arrays.asList(desc)))
					._titles_(new ArrayList<>(Arrays.asList(title)))
					.build();
		} catch (MalformedURLException e) {
			LOGGER.error("Exception:",e);
		} catch (ConstraintViolationException e) {
			LOGGER.error("Exception:",e);
		}
		return agent;
	}

	private ArrayList<PlainLiteral> getKeyWords(DataAsset da) {
		ArrayList<PlainLiteral> keywords = new ArrayList<>();
		for (String tag : da.getTags()) {
			keywords.add(new PlainLiteral(tag));
		}
		return keywords.isEmpty()? null : keywords;
	}
}
