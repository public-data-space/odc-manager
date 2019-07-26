package de.fraunhofer.fokus.ids.models;

import de.fraunhofer.fokus.ids.persistence.enums.DatasourceType;

import java.util.Map;

public class DataAssetDescription {

    private int sourceId;
    private Map data;
    private DatasourceType datasourcetype;

    public int getSourceId() {
        return sourceId;
    }

    public void setSourceId(int sourceId) {
        this.sourceId = sourceId;
    }

    public Map getData() {
        return data;
    }

    public void setData(Map data) {
        this.data = data;
    }

    public DatasourceType getDatasourcetype() {
        return datasourcetype;
    }

    public void setDatasourcetype(DatasourceType datasourcetype) {
        this.datasourcetype = datasourcetype;
    }


}
