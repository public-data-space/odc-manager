package de.fraunhofer.fokus.ids.models;

import java.util.List;
import java.util.Map;

public class FileResponse {
    private Map<String, List<String>> headers;
    private String body;

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, List<String>> headers) {
        this.headers = headers;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

}
