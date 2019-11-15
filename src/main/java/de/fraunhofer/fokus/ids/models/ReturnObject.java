package de.fraunhofer.fokus.ids.models;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class ReturnObject {

	private String entity;
	private String type;
	
	public ReturnObject() { }

	public ReturnObject(String entity, String type) {
		this.type= type;
		this.entity = entity;
	}
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type= type;
	}

	public String getEntity() {
		return entity;
	}

	public void setEntity(String entity) {
		this.entity = entity;
	}

}
