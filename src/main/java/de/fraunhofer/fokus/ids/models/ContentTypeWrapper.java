package de.fraunhofer.fokus.ids.models;

public class ContentTypeWrapper {

    private String contentType;

    private String jenaFormat;
    
    public ContentTypeWrapper() { }

    public ContentTypeWrapper(String contentType, String jenaFormat) {
        this.contentType = contentType;
        this.jenaFormat = jenaFormat;
    }

    public String getContentType() {
        return contentType;
    }

    public String getJenaFormat() {
        return jenaFormat;
    }

}
