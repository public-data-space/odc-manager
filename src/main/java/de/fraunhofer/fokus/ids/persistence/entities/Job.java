package de.fraunhofer.fokus.ids.persistence.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.fraunhofer.fokus.ids.persistence.enums.DatasourceType;
import de.fraunhofer.fokus.ids.persistence.enums.JobStatus;
import io.vertx.core.json.JsonObject;

public class Job extends BaseEntity {


    @JsonProperty("data")
    private JsonObject data;

    @JsonProperty("sourceid")
    private String sourceID;

    @JsonProperty("status")
    private JobStatus status;

    @JsonProperty("sourcetype")
    private DatasourceType sourceType;

    public JobStatus getStatus() {
        return status;
    }

    public void setStatus(JobStatus status) {
        this.status = status;
    }

    public String getSourceID() {
        return sourceID;
    }

    public void setSourceID(String sourceID) {
        this.sourceID = sourceID;
    }

    public JsonObject getData() {
        return data;
    }

    public void setData(JsonObject data) {
        this.data = data;
    }

    public DatasourceType getSourceType() {
        return sourceType;
    }

    public void setSourceType(DatasourceType sourceType) {
        this.sourceType = sourceType;
    }

}
