package de.fraunhofer.fokus.ids.models;

public class DataRequest {
	
	String id;
	String extension;

	public DataRequest() {}
	
	public DataRequest(String id, String extension) {
		this.id = id;
		this.extension = extension;
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getExtension() {
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}

}
