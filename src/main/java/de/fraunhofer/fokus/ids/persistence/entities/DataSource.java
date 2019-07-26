package de.fraunhofer.fokus.ids.persistence.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.fraunhofer.fokus.ids.persistence.enums.DatasourceType;
import io.vertx.core.json.JsonObject;

public class DataSource extends BaseEntity {

    @JsonProperty("data")
    private JsonObject data;
    @JsonProperty("datasourcename")
    String datasourceName;
    @JsonProperty("datasourcetype")
    DatasourceType datasourceType;

    public JsonObject getData() {
        return data;
    }

    public void setData(JsonObject data) {
        this.data = data;
    }


    public String getDatasourceName() {
        return datasourceName;
    }

    public void setDatasourceName(String datasourceName) {
        this.datasourceName = datasourceName;
    }

    public DatasourceType getDatasourceType() {
        return datasourceType;
    }

    public void setDatasourceType(DatasourceType datasourceType) {
        this.datasourceType = datasourceType;
    }

}
