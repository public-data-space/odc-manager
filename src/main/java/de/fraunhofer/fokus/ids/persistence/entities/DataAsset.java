package de.fraunhofer.fokus.ids.persistence.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.fraunhofer.fokus.ids.persistence.enums.DataAssetStatus;

import java.util.List;

public class DataAsset extends BaseEntity {

    @JsonProperty("datasetid")
    private String datasetID;

    @JsonProperty("sourceid")
    private String sourceID;

    private String name;

    private String url;

    private String format;
    @JsonProperty("licenseurl")
    private String licenseUrl;
    
    @JsonProperty("licensetitle")
    private String licenseTitle;
    
    @JsonProperty("datasettitle")
    private String datasetTitle;
    
    @JsonProperty("datasetnotes")
    private String datasetNotes;
    
    @JsonProperty("orignalresourceurl")
    private String orignalResourceURL;
    
    @JsonProperty("orignaldataseturl")
    private String orignalDatasetURL;

    @JsonProperty("accessinformation")
    private String accessInformation;

    private String signature;

    private DataAssetStatus status;
    
    @JsonProperty("resourceid")
    private String resourceID;

    private List<String> tags;

    @JsonProperty("datasetdescription")
    private String dataSetDescription;

    @JsonProperty("organizationtitle")
    private String organizationTitle;

    @JsonProperty("organizationdescription")
    private String organizationDescription;

	private String version;

    public String getSourceID() {
        return sourceID;
    }

    public void setSourceID(String sourceID) {
        this.sourceID = sourceID;
    }

    public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getOrganizationTitle() {
		return organizationTitle;
	}

	public void setOrganizationTitle(String organizationTitle) {
		this.organizationTitle = organizationTitle;
	}

    public String getOrganizationDescription() {
		return organizationDescription;
	}

	public void setOrganizationDescription(String organizationDescription) {
		this.organizationDescription = organizationDescription;
	}

	public String getDataSetDescription() {
		return dataSetDescription;
	}

	public void setDataSetDescription(String description) {
		this.dataSetDescription = description;
	}

	public String getResourceID() {
        return resourceID;
    }

    public void setResourceID(String resourceID) {
        this.resourceID = resourceID;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getLicenseUrl() {
        return licenseUrl;
    }

    public void setLicenseUrl(String licenseUrl) {
        this.licenseUrl = licenseUrl;
    }

    public String getLicenseTitle() {
        return licenseTitle;
    }

    public void setLicenseTitle(String licenseTitle) {
        this.licenseTitle = licenseTitle;
    }

    public String getDatasetTitle() {
        return datasetTitle;
    }

    public void setDatasetTitle(String datasetTitle) {
        this.datasetTitle = datasetTitle;
    }

    public String getDatasetNotes() {
        return datasetNotes;
    }

    public void setDatasetNotes(String datasetNotes) {
        this.datasetNotes = datasetNotes;
    }

    public String getOrignalResourceURL() {
        return orignalResourceURL;
    }

    public void setOrignalResourceURL(String orignalResourceURL) {
        this.orignalResourceURL = orignalResourceURL;
    }

    public String getOrignalDatasetURL() {
        return orignalDatasetURL;
    }

    public void setOrignalDatasetURL(String orignalDatasetURL) {
        this.orignalDatasetURL = orignalDatasetURL;
    }

    public String getAccessInformation() {
        return accessInformation;
    }

    public void setAccessInformation(String accessInformation) {
        this.accessInformation = accessInformation;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public DataAssetStatus getStatus() {
        return status;
    }

    public void setStatus(DataAssetStatus status) {
        this.status = status;
    }

    public String getDatasetID() {
        return datasetID;
    }

    public void setDatasetID(String datasetID) {
        this.datasetID = datasetID;
    }

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}
}
