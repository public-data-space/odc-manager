package de.fraunhofer.fokus.ids.services.webclient;

import de.fraunhofer.fokus.ids.models.Constants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;

public class WebClientServiceVerticle extends AbstractVerticle {

    private Logger LOGGER = LoggerFactory.getLogger(WebClientServiceVerticle.class.getName());

    @Override
    public void start(Future<Void> startFuture) {
        WebClient webClient = WebClient.create(vertx);

        WebClientService.create(webClient, ready -> {
            if (ready.succeeded()) {
                ServiceBinder binder = new ServiceBinder(vertx);
                binder
                        .setAddress(Constants.WEBCLIENT_SERVICE)
                        .register(WebClientService.class, ready.result());
                startFuture.complete();
            } else {
                startFuture.fail(ready.cause());
            }
        });
    }
}
