package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.persistence.managers.BrokerManager;
import de.fraunhofer.iais.eis.*;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.WebClient;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.StringBody;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class BrokerService {

    private final Logger LOGGER = LoggerFactory.getLogger(BrokerService.class.getName());

    private BrokerManager brokerManager;
    private IDSService idsService;
    private WebClient webClient;

    public BrokerService(Vertx vertx){
        this.brokerManager = new BrokerManager(vertx);
        this.idsService = new IDSService(vertx);
        this.webClient = WebClient.create(vertx);
    }

    public void subscribe(String url, Handler<AsyncResult<Void>> resultHandler){
        Future<Connector> connectorFuture = Future.future();
        Future<ConnectorAvailableMessage> messageFuture = Future.future();
        idsService.getConnector(connectorFuture);
        idsService.createRegistrationMessage(messageFuture);

        CompositeFuture.all(connectorFuture, messageFuture).setHandler(reply -> {
            if(reply.succeeded()) {
                List<URL> urls = new ArrayList<>();
                try {
                    urls.add(new URL(url));
                    sendMessage(createBrokerMessage(messageFuture.result(), connectorFuture.result()), urls, resultHandler);
                } catch (MalformedURLException e) {
                    LOGGER.error(e);
                    resultHandler.handle(Future.failedFuture(e));
                }
            }
            else{
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    public void unsubscribe(String url, Handler<AsyncResult<Void>> resultHandler){
        Future<Connector> connectorFuture = Future.future();
        Future<ConnectorUnavailableMessage> messageFuture = Future.future();
        idsService.getConnector(connectorFuture);
        idsService.createUnregistrationMessage(messageFuture);

        CompositeFuture.all(connectorFuture, messageFuture).setHandler(reply -> {
            if(reply.succeeded()) {
                List<URL> urls = new ArrayList<>();
                try {
                    urls.add(new URL(url));
                    sendMessage(createBrokerMessage(messageFuture.result(), connectorFuture.result()), urls, resultHandler);
                } catch (MalformedURLException e) {
                    LOGGER.error(e);
                    resultHandler.handle(Future.failedFuture(e));
                }
            }
            else{
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    public void update(Handler<AsyncResult<Void>> resultHandler){
        Future<Connector> connectorFuture = Future.future();
        Future<ConnectorUpdateMessage> messageFuture = Future.future();
        idsService.getConnector(connectorFuture);
        idsService.createUpdateMessage(messageFuture);

        CompositeFuture.all(connectorFuture, messageFuture).setHandler(reply -> {
            if(reply.succeeded()){
                getBrokerURLs(reply2 -> {
                    if (reply2.succeeded()) {
                        sendMessage(createBrokerMessage(messageFuture.result(), connectorFuture.result()), reply2.result(), resultHandler);
                    }
                    else{
                        resultHandler.handle(Future.failedFuture(reply2.cause()));
                    }
                });
            }
            else{
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    private void sendMessage(Buffer buffer, List<URL> urls, Handler<AsyncResult<Void>> resultHandler){
        if(buffer != null) {
            for (URL url : urls) {
                final int port = url.getPort() == -1 ? 80 : url.getPort();
                final String host = url.getHost();
                final String path = url.getPath();

                webClient
                        .post(port, host, path)
                        .sendBuffer(buffer, ar -> {
                            if (ar.succeeded()) {
                                resultHandler.handle(Future.succeededFuture());
                            } else {
                                LOGGER.error(ar.cause());
                                resultHandler.handle(Future.failedFuture(ar.cause()));
                            }
                        });
            }
        }
        else{
            resultHandler.handle(Future.failedFuture("Message could not be created."));
        }
    }

    private void getBrokerURLs(Handler<AsyncResult<List<URL>>> resultHandler){
        brokerManager.findAll( reply -> {
            if (reply.succeeded()){
                List<URL> brokerUrls = new ArrayList<>();
                for(int i =0;i<reply.result().size();i++) {
                    try {
                        brokerUrls.add(new URL(reply.result().getJsonObject(i).getString("url")));
                    } catch (MalformedURLException e) {
                        LOGGER.error(e);
                        resultHandler.handle(Future.succeededFuture(new ArrayList<>()));
                    }
                }
                resultHandler.handle(Future.succeededFuture(brokerUrls));
            }
            else {
                resultHandler.handle(Future.succeededFuture(new ArrayList<>()));
            }
        });
    }

   private Buffer createBrokerMessage(ConnectorNotificationMessage message, Connector connector){
       ContentBody cb = new StringBody(Json.encodePrettily(message), org.apache.http.entity.ContentType.create("application/json"));
       ContentBody result = new StringBody(Json.encodePrettily(connector), org.apache.http.entity.ContentType.create("application/json"));

       MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
               .setBoundary("IDSMSGPART")
               .setCharset(StandardCharsets.UTF_8)
               .setContentType(ContentType.APPLICATION_JSON)
               .addPart("header", cb)
               .addPart("payload", result);

       ByteArrayOutputStream out = new ByteArrayOutputStream();
       try {
           multipartEntityBuilder.build().writeTo(out);
           return Buffer.buffer().appendString(out.toString());
       } catch (IOException e) {
           LOGGER.error(e);
       }
       return null;
    }
}
