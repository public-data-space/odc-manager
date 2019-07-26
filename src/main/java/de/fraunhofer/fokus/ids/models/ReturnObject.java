package de.fraunhofer.fokus.ids.models;

public class ReturnObject {

	private String entity;
	private ContentTypeWrapper typeWrapper;
	
	public ReturnObject() { }

	public ReturnObject(String entity, ContentTypeWrapper typeWrapper) {
		this.entity = entity;
		this.typeWrapper = typeWrapper;
	}
	public ContentTypeWrapper getTypeWrapper() {
		return typeWrapper;
	}
	public void setTypeWrapper(ContentTypeWrapper typeWrapper) {
		this.typeWrapper = typeWrapper;
	}
	public String getEntity() {
		return entity;
	}
	public void setEntity(String entity) {
		this.entity = entity;
	}

}
