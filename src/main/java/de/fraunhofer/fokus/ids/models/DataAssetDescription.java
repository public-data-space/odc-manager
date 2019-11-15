package de.fraunhofer.fokus.ids.models;

import java.util.Map;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DataAssetDescription {

    private int sourceId;
    private Map data;
    private String datasourcetype;

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

    public String getDatasourcetype() {
        return datasourcetype;
    }

    public void setDatasourcetype(String datasourcetype) {
        this.datasourcetype = datasourcetype;
    }


}
