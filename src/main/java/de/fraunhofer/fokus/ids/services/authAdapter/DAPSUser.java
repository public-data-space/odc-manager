package de.fraunhofer.fokus.ids.services.authAdapter;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AbstractUser;
import io.vertx.ext.auth.AuthProvider;
import org.jose4j.jwt.JwtClaims;

public class DAPSUser extends AbstractUser {

    private JwtClaims jwtClaims;

    public DAPSUser(JwtClaims jwtClaims) {

        this.jwtClaims = jwtClaims;
    }

    @Override
    protected void doIsPermitted(String s, Handler<AsyncResult<Boolean>> handler) {


    }

    @Override
    public JsonObject principal() {
        return new JsonObject(jwtClaims.getClaimsMap());
    }

    @Override
    public void setAuthProvider(AuthProvider authProvider) {

    }
}
