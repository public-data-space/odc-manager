package de.fraunhofer.fokus.ids.services.authAdapter;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;

import java.nio.file.Path;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
@ProxyGen
@VertxGen
public interface AuthAdapterService {

    @Fluent
    AuthAdapterService retrieveToken(Handler<AsyncResult<String>> readyHandler);

    @GenIgnore
    static AuthAdapterService create(Vertx vertx,
                                     WebClient webClient,
                                     Path targetDirectory,
                                     String keyStoreName,
                                     String keyStorePassword,
                                     String keystoreAliasName,
                                     String trustStoreName,
                                     String connectorUUID,
                                     String dapsUrl,
                                     Handler<AsyncResult<AuthAdapterService>> readyHandler) {
        return new AuthAdapterServiceImpl(vertx, webClient, targetDirectory, keyStoreName, keyStorePassword, keystoreAliasName, trustStoreName, connectorUUID, dapsUrl, readyHandler);
    }

    @GenIgnore
    static AuthAdapterService createProxy(Vertx vertx, String address) {
        return new AuthAdapterServiceVertxEBProxy(vertx, address);
    }

}
