package de.fraunhofer.fokus.ids.persistence.managers;

import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.persistence.entities.User;
import de.fraunhofer.fokus.ids.persistence.service.DatabaseService;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.jwt.JWTOptions;
import org.mindrot.jbcrypt.BCrypt;

import java.util.Arrays;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class AuthManager {

    private Logger LOGGER = LoggerFactory.getLogger(AuthManager.class.getName());
    private JWTAuth provider;

    private DatabaseService dbService;

    private static final String USER_QUERY = "SELECT * FROM public.user WHERE username = ?";

    public AuthManager(Vertx vertx) {
        dbService = DatabaseService.createProxy(vertx, Constants.DATABASE_SERVICE);
        provider = JWTAuth.create(vertx, new JWTAuthOptions()
                .addPubSecKey(new PubSecKeyOptions()
                        .setAlgorithm("HS256")
                        .setPublicKey("keyboard cat")
                        .setSymmetric(true)));

    }

    public JWTAuth getProvider(){
        return provider;
    }

    public void login(JsonObject credentials, Handler<AsyncResult<String>> resultHandler) {

        dbService.query(USER_QUERY,  new JsonArray(Arrays.asList(credentials.getString("username"))), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                if (reply.result().size() > 0) {
                    User user = Json.decodeValue(reply.result().get(0).toString(), User.class);

                    if (BCrypt.checkpw(credentials.getString("password"), user.getPassword())) {
                        resultHandler.handle(Future.succeededFuture(provider.generateToken(new JsonObject().put("sub", user.getUsername()), new JWTOptions().setExpiresInMinutes(60))));
                    } else {
                        resultHandler.handle(Future.failedFuture("Password is not identical to password in database."));
                    }
                }
                else {
                    resultHandler.handle(Future.failedFuture("User is not registered in the database."));
                }
            }
        });

    }

}
