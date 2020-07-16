package de.fraunhofer.fokus.ids.services.authAdapter;

import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.services.datasourceAdapter.DataSourceAdapterServiceVerticle;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.serviceproxy.ServiceBinder;

import java.nio.file.Path;
import java.nio.file.Paths;

public class AuthAdapterServiceVerticle extends AbstractVerticle {

    private Logger LOGGER = LoggerFactory.getLogger(DataSourceAdapterServiceVerticle.class.getName());

    @Override
    public void start(Promise<Void> startPromise) {

        ConfigStoreOptions confStore = new ConfigStoreOptions()
                .setType("env");

        ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(confStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        retriever.getConfig(ar -> {
            if (ar.succeeded()) {
                Path path = Paths.get("/ids/certs/");
                JksOptions jksOptions = new JksOptions();
                Buffer store = vertx.fileSystem().readFileBlocking(path.resolve(ar.result().getString("TRUSTSTORE_NAME")).toString());
                jksOptions.setValue(store).setPassword(ar.result().getString("KEYSTORE_PASSWORD"));
                WebClient webClient = WebClient.create(vertx, new WebClientOptions().setTrustStoreOptions(jksOptions));


                AuthAdapterService.create(vertx, webClient, path, ar.result().getString("KEYSTORE_NAME"), ar.result().getString("KEYSTORE_PASSWORD"), ar.result().getString("KEYSTORE_ALIAS_NAME"), ar.result().getString("TRUSTSTORE_NAME"), ar.result().getString("CONNECTOR_UUID"), ar.result().getString("DAPS_URL"), ready -> {
                    if (ready.succeeded()) {
                        ServiceBinder binder = new ServiceBinder(vertx);
                        binder
                                .setAddress(Constants.AUTHADAPTER_SERVICE)
                                .register(AuthAdapterService.class, ready.result());
                        LOGGER.info("AuthAdapterservice successfully started.");
                        startPromise.complete();
                    } else {
                        LOGGER.error(ready.cause());
                        startPromise.fail(ready.cause());
                    }
                });
            } else {
                LOGGER.error(ar.cause());
                startPromise.fail(ar.cause());
            }
        });
    }
}