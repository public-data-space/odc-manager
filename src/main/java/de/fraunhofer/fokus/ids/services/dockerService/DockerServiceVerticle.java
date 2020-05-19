package de.fraunhofer.fokus.ids.services.dockerService;

import de.fraunhofer.fokus.ids.models.Constants;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;

public class DockerServiceVerticle extends AbstractVerticle {
    private Logger LOGGER = LoggerFactory.getLogger(DockerServiceVerticle.class.getName());

    @Override
    public void start(Future<Void> startFuture) {
        WebClient webClient = WebClient.create(vertx);

        ConfigStoreOptions confStore = new ConfigStoreOptions()
                .setType("env");

        ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(confStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        retriever.getConfig(ar -> {
            if (ar.succeeded()) {
                DockerService.create(vertx, webClient, ar.result().getInteger("CONFIG_MANAGER_PORT"), ar.result().getString("CONFIG_MANAGER_HOST"), ar.result().getString("REPOSITORY"), ready -> {
                    if (ready.succeeded()) {
                        ServiceBinder binder = new ServiceBinder(vertx);
                        binder
                                .setAddress(Constants.DOCKER_SERVICE)
                                .register(DockerService.class, ready.result());
                        LOGGER.info("DockerService successfully started.");
                        startFuture.complete();
                    } else {
                        LOGGER.error(ready.cause());
                        startFuture.fail(ready.cause());
                    }
                });
            } else {
                LOGGER.error(ar.cause());
                startFuture.fail(ar.cause());
            }
        });
    }
}
