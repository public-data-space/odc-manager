package de.fraunhofer.fokus.ids.services.authAdapter;

import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.client.WebClient;
import org.jose4j.http.Get;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.ErrorCodes;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.keys.resolvers.HttpsJwksVerificationKeyResolver;

import javax.net.ssl.*;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;

public class AuthAdapterServiceImpl implements AuthAdapterService, AuthProvider {


    private Logger LOGGER = LoggerFactory.getLogger(AuthAdapterServiceImpl.class.getName());
    private SSLSocketFactory sslSocketFactory = null;
    private String connectorUUID;
    private Key privKey;
    private X509Certificate cert;
    private Vertx vertx;
    private String dapsUrl;
    private String targetAudience = "idsc:IDS_CONNECTORS_ALL";
    private WebClient webClient;

    public AuthAdapterServiceImpl(Vertx vertx,
                                  WebClient webClient,
                                  Path targetDirectory,
                                  String keyStoreName,
                                  String keyStorePassword,
                                  String keystoreAliasName,
                                  String trustStoreName,
                                  String connectorUUID,
                                  String dapsUrl,
                                  Handler<AsyncResult<AuthAdapterService>> readyHandler) {
        {
            this.webClient = webClient;
            this.connectorUUID = connectorUUID;
            this.vertx = vertx;
            this.dapsUrl = dapsUrl;

            // Try clause for setup phase (loading keys, building trust manager)
            try {
                InputStream jksKeyStoreInputStream =
                        Files.newInputStream(targetDirectory.resolve(keyStoreName));
                InputStream jksTrustStoreInputStream =
                        Files.newInputStream(targetDirectory.resolve(trustStoreName));

                KeyStore keystore = KeyStore.getInstance("PKCS12");
                KeyStore trustManagerKeyStore = KeyStore.getInstance("PKCS12");

                LOGGER.info("Loading key store: " + keyStoreName);
                LOGGER.info("Loading trust store: " + trustStoreName);
                keystore.load(jksKeyStoreInputStream, keyStorePassword.toCharArray());
                trustManagerKeyStore.load(jksTrustStoreInputStream, keyStorePassword.toCharArray());
                java.security.cert.Certificate[] certs = trustManagerKeyStore.getCertificateChain("aisecdaps");
                LOGGER.info("Cert chain: " + Arrays.toString(certs));

                LOGGER.info("LOADED CA CERT: " + trustManagerKeyStore.getCertificate("aisecdaps"));
                jksKeyStoreInputStream.close();
                jksTrustStoreInputStream.close();

                // get private key
                this.privKey = keystore.getKey(keystoreAliasName, keyStorePassword.toCharArray());
                // Get certificate of public key
                LOGGER.info(keystoreAliasName);
                this.cert = (X509Certificate) keystore.getCertificate(keystoreAliasName);

                TrustManager[] trustManagers;
                try {
                    TrustManagerFactory trustManagerFactory =
                            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    trustManagerFactory.init(trustManagerKeyStore);
                    trustManagers = trustManagerFactory.getTrustManagers();
                    if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
                        readyHandler.handle(Future.failedFuture(new IllegalStateException(
                                "Unexpected default trust managers:" + Arrays.toString(trustManagers))));
                    }
                    SSLContext sslContext = SSLContext.getInstance("TLS");
                    sslContext.init(null, trustManagers, null);
                    sslSocketFactory = sslContext.getSocketFactory();
                } catch (GeneralSecurityException e) {
                    readyHandler.handle(Future.failedFuture(e));
                }

                LOGGER.info("\tCertificate Subject: " + cert.getSubjectDN());
                //GET 2.5.29.14	SubjectKeyIdentifier / 2.5.29.35	AuthorityKeyIdentifier
                String authorityKeyIndentifier = new String(cert.getExtensionValue("2.5.29.35"));
                String subjectKeyIdenfier = new String(cert.getExtensionValue("2.5.29.14"));
                LOGGER.info("AKI: " + authorityKeyIndentifier);
                LOGGER.info("SKI: " + subjectKeyIdenfier);
                //connectorUUID = subjectKeyIdenfier + ":" + authorityKeyIndentifier.substring(0, authorityKeyIndentifier.length() - 1);
                LOGGER.info("ConnectorUUID: " + connectorUUID);
            } catch (KeyStoreException
                    | NoSuchAlgorithmException
                    | CertificateException
                    | UnrecoverableKeyException e) {
                LOGGER.error("Cannot acquire token:", e);
                readyHandler.handle(Future.failedFuture(e));
            } catch (IOException e) {
                LOGGER.error("Error retrieving token:", e);
                readyHandler.handle(Future.failedFuture(e));
            } catch (Exception e) {
                LOGGER.error("Something else went wrong:", e);
                readyHandler.handle(Future.failedFuture(e));
            }
        }
        readyHandler.handle(Future.succeededFuture(this));
    }

    @Override
    public void authenticate(JsonObject jsonObject, Handler<AsyncResult<User>> handler) {
        verifyJWT(jsonObject.getString("jwt"),jsonObject.getString("audience"),  reply -> {
            if(reply.succeeded()){
                handler.handle(Future.succeededFuture(new DAPSUser(reply.result())));
            } else {
                handler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    private void verifyJWT(
            String dynamicAttributeToken,
            String targetAudience,
            Handler<AsyncResult<JwtClaims>> resultHandler) {
        if (sslSocketFactory == null) {
            throw new JwtException("SSLSocketFactory is null, acquireToken() must be called first!");
        }

        try {
            // The HttpsJwks retrieves and caches keys from a the given HTTPS JWKS endpoint.
            // Because it retains the JWKs after fetching them, it can and should be reused
            // to improve efficiency by reducing the number of outbound calls the the endpoint.
            HttpsJwks httpsJkws = new HttpsJwks(dapsUrl + "/.well-known/jwks.json");
            Get getInstance = new Get();
            getInstance.setSslSocketFactory(sslSocketFactory);
            httpsJkws.setSimpleHttpGet(getInstance);

            // The HttpsJwksVerificationKeyResolver uses JWKs obtained from the HttpsJwks and will select
            // the most appropriate one to use for verification based on the Key ID and other factors
            // provided in the header of the JWS/JWT.
            HttpsJwksVerificationKeyResolver httpsJwksKeyResolver =
                    new HttpsJwksVerificationKeyResolver(httpsJkws);

            // Use JwtConsumerBuilder to construct an appropriate JwtConsumer, which will
            // be used to validate and process the JWT.
            // The specific validation requirements for a JWT are context dependent, however,
            // it typically advisable to require a (reasonable) expiration time, a trusted issuer, and
            // and audience that identifies your system as the intended recipient.
            // If the JWT is encrypted too, you need only provide a decryption key or
            // decryption key resolver to the builder.
            JwtConsumer jwtConsumer =
                    new JwtConsumerBuilder()
                            .setRequireExpirationTime() // the JWT must have an expiration time
                            .setAllowedClockSkewInSeconds(
                                    30) // allow some leeway in validating time based claims to account for clock skew
                            .setRequireSubject() // the JWT must have a subject claim
                            .setExpectedIssuer(
                                    "https://daps.aisec.fraunhofer.de") // whom the JWT needs to have been issued by
                            .setExpectedAudience(targetAudience) // to whom the JWT is intended for
                            .setVerificationKeyResolver(httpsJwksKeyResolver)
                            .setJwsAlgorithmConstraints( // only allow the expected signature algorithm(s) in the
                                    // given context
                                    new org.jose4j.jwa.AlgorithmConstraints(
                                            org.jose4j.jwa.AlgorithmConstraints.ConstraintType
                                                    .WHITELIST, // which is only RS256 here
                                            AlgorithmIdentifiers.RSA_USING_SHA256))
                            .build(); // create the JwtConsumer instance

            LOGGER.info("Verifying JWT...");
            //  Validate the JWT and process it to the Claims
            JwtClaims jwtClaims = jwtConsumer.processToClaims(dynamicAttributeToken);
            LOGGER.info("JWT validation succeeded! " + jwtClaims);

            resultHandler.handle(Future.succeededFuture(jwtClaims));

        } catch (InvalidJwtException e) {
            // InvalidJwtException will be thrown, if the JWT failed processing or validation in anyway.
            // Hopefully with meaningful explanations(s) about what went wrong.
            LOGGER.warn("Invalid JWT!", e);

            // Programmatic access to (some) specific reasons for JWT invalidity is also possible
            // should you want different error handling behavior for certain conditions.

            // Whether or not the JWT has expired being one common reason for invalidity
            if (e.hasExpired()) {
                try {
                    LOGGER.warn("JWT expired at " + e.getJwtContext().getJwtClaims().getExpirationTime());
                } catch (MalformedClaimException e1) {
                    LOGGER.error("Malformed claim encountered", e1);
                    resultHandler.handle(Future.failedFuture(e1));
                }
            }

            // Or maybe the audience was invalid
            if (e.hasErrorCode(ErrorCodes.AUDIENCE_INVALID)) {
                try {
                    LOGGER.warn("JWT had wrong audience: " + e.getJwtContext().getJwtClaims().getAudience());
                } catch (MalformedClaimException e1) {
                    LOGGER.error("Malformed claim encountered", e1);
                    resultHandler.handle(Future.failedFuture(e1));
                }
            }
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    @Override
    public AuthAdapterService retrieveToken(Handler<AsyncResult<String>> resultHandler){

        LOGGER.info("Retrieving Dynamic Attribute Token...");

        // create signed JWT (JWS)
        // Create expiry date one day (86400 seconds) from now
        Date expiryDate = Date.from(Instant.now().plusSeconds(86400));
        JwtBuilder jwtb =
                Jwts.builder()
                        .setIssuer(connectorUUID)
                        .setSubject(connectorUUID)
                        .setExpiration(expiryDate)
                        .setIssuedAt(Date.from(Instant.now()))
                        .setAudience(targetAudience)
                        .setNotBefore(Date.from(Instant.now()));
        LOGGER.info("\tCertificate Subject: " + cert.getSubjectDN());
        String jws = jwtb.signWith(privKey, SignatureAlgorithm.RS256).compact();

        webClient
                .post(80, dapsUrl,  "/token")
                .putHeader("grant_type", "client_credentials")
                .putHeader("client_assertion_type", "urn:ietf:params:oauth:client-assertion-type:jwt-bearer")
                .putHeader("client_assertion", jws)
                .putHeader("scope", "ids_connector")
                .send( ar -> {
                    if (ar.succeeded()) {
                        JsonObject jwtJson = ar.result().bodyAsJsonObject();
                        String dynamicAttributeToken = jwtJson.getString("access_token");
                        LOGGER.info("Dynamic Attribute Token: " + dynamicAttributeToken);
                        verifyJWT(dynamicAttributeToken, targetAudience, ac ->{
                            if(ac.succeeded()){
                                resultHandler.handle(Future.succeededFuture(dynamicAttributeToken));
                            } else {
                                LOGGER.error(ar.cause());
                                resultHandler.handle(Future.failedFuture(ar.cause()));
                            }
                        });
                    } else {
                        LOGGER.error(ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
        return this;
    }
}
