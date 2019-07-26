package de.fraunhofer.fokus.ids.persistence.util;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class Functions {

    private static Logger LOGGER = LoggerFactory.getLogger(Functions.class.getName());

    public static String checkNull(String toTest){
        return toTest == null ? "" : toTest;
    }

}
