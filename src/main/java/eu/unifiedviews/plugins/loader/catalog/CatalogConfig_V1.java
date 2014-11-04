package eu.unifiedviews.plugins.loader.catalog;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class CatalogConfig_V1 {
    private String catalogApiLocation = "http://localhost:5000/uv/";

    private String datasetId = "dataset";

    private boolean skipOnError = false;

    public CatalogConfig_V1() {
    }

    public String getCatalogApiLocation() {
        return catalogApiLocation;
    }

    public void setCatalogApiLocation(String catalogApiLocation) {
        this.catalogApiLocation = catalogApiLocation;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public boolean isSkipOnError() {
        return skipOnError;
    }

    public void setSkipOnError(boolean skipOnError) {
        this.skipOnError = skipOnError;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE).toString();
    }
}
