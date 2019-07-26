package de.fraunhofer.fokus.ids.services;//package de.fraunhofer.fokus.ids.services;
//
//import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
//import io.vertx.config.ConfigRetriever;
//import io.vertx.config.ConfigRetrieverOptions;
//import io.vertx.config.ConfigStoreOptions;
//import io.vertx.core.AbstractVerticle;
//import io.vertx.core.Future;
//import io.vertx.core.eventbus.EventBus;
//import io.vertx.core.eventbus.Message;
//import io.vertx.core.json.Json;
//import io.vertx.core.json.JsonObject;
//import io.vertx.core.logging.Logger;
//import io.vertx.core.logging.LoggerFactory;
//
//
//public class URLService extends AbstractVerticle {
//
//    final private Logger LOGGER = LoggerFactory.getLogger(URLService.class.getName());
//	private String ROUTE_PREFIX = "de.fraunhofer.fokus.ids.urlService.";
//    private EventBus eb;
//    private JsonObject config;
//
//    @Override
//	public void start(Future<Void> startFuture) {
////    	getConfig();
//    	this.eb = vertx.eventBus();
//    	eb.consumer(ROUTE_PREFIX+"getResourceURL", receivedMessage -> {
//    		getResourceURL(receivedMessage);
//		});
//    	eb.consumer(ROUTE_PREFIX+"getOriginalResourceURL", receivedMessage -> {
//    		getOriginalResourceURL(receivedMessage);
//		});
//    	eb.consumer(ROUTE_PREFIX+"getPayloadURL", receivedMessage -> {
//    		getPayloadURL(receivedMessage);
//		});
//    	eb.consumer(ROUTE_PREFIX+"getConnectorURL", receivedMessage -> {
//    		getConnectorURL(receivedMessage);
//		});
//    }
//
//    public void getResourceURL(Message<Object> receivedMessage) {
//    	DataAsset dataAsset = Json.decodeValue(receivedMessage.body().toString(), DataAsset.class);
//        return controllers.routes.DataAssetController.resource(dataAsset.getFile())
//                .absoluteURL(Http.Context.current().request());
//    }
//
//    public void getOriginalResourceURL(Message<Object> receivedMessage) {
//    	DataAsset dataAsset = Json.decodeValue(receivedMessage.body().toString(), DataAsset.class);
//    	
//        receivedMessage.reply(config.getString("ckan.url")
//                + "dataset/"
//                + dataAsset.getDatasetID()
//                + "/resource/"
//                + dataAsset.getResourceID());
//    }
//
//    public void getPayloadURL(Message<Object> receivedMessage) {
//    	JsonObject jO = Json.decodeValue(receivedMessage.body().toString(), JsonObject.class);
//    	String format = jO.getString("format");
//    	DataAsset dataAsset = Json.decodeValue(jO.getJsonObject("dataAsset").toString(), DataAsset.class);
//        if(format == null) {
//            return controllers.routes.ConnectorController.payload(dataAsset.getId())
//                    .absoluteURL(Http.Context.current().request());
//        } else {
//            return routes.ConnectorController.payloadContent(dataAsset.getId(), format)
//                    .absoluteURL(Http.Context.current().request());
//        }
//    }
//
//    public void getConnectorURL(Message<Object> receivedMessage) {
//    	String extension = receivedMessage.body().toString();
//        String baseURL = controllers.routes.ConnectorController.connector(".txt").url();
//        if(extension != null) {
//            return baseURL + "." + extension;
//        } else {
//            return baseURL;
//        }
//    }
//    
////	private void getConfig() {
////	eb.send(ROUTE_PREFIX+"repositoryService.configRetrieverVerticle.getConfig", "", res -> {
////		if(res.succeeded()) {
////			JsonObject config = Json.decodeValue(res.result().body().toString(),JsonObject.class);
////			this.repoPath = config.getString("repository");
////		}
////		else {
////			LOGGER.error("Config could not be loaded.");
////		}
////	});
////}
//
//}
