package eu.unifiedviews.plugins.loader.catalog;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class CatalogConfig_V1 {
    private String catalogApiLocation = "http://localhost/internalcatalog/uv";

    public CatalogConfig_V1() {
    }

    public String getCatalogApiLocation() {
        return catalogApiLocation;
    }

    public void setCatalogApiLocation(String catalogApiLocation) {
        this.catalogApiLocation = catalogApiLocation;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE).toString();
    }
}
