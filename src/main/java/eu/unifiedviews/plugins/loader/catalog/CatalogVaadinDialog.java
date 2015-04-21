package eu.unifiedviews.plugins.loader.catalog;

import com.vaadin.data.util.ObjectProperty;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.TextField;

import eu.unifiedviews.dpu.config.DPUConfigException;
import eu.unifiedviews.helpers.dpu.vaadin.dialog.AbstractDialog;

/**
 * DPU's configuration dialog. User can use this dialog to configure DPU
 * configuration.
 */
public class CatalogVaadinDialog extends AbstractDialog<CatalogConfig_V1> {

    private static final long serialVersionUID = -5668436075836909428L;

    private static final String PIPELINE_ID_LABEL = "Pipeline ID";

    private ObjectProperty<String> catalogApiLocation = new ObjectProperty<String>("");

    private ObjectProperty<Long> pipelineId = new ObjectProperty<Long>(0L);

    public CatalogVaadinDialog() {
        super(Catalog.class);
    }

    @Override
    protected void buildDialogLayout() {
        FormLayout mainLayout = new FormLayout();

        // top-level component properties
        setWidth("100%");
        setHeight("100%");

        TextField txtApiLocation = new TextField(ctx.tr("catalog.dialog.api.location"), catalogApiLocation);
        txtApiLocation.setWidth("100%");
        mainLayout.addComponent(txtApiLocation);
        mainLayout.setSpacing(true);
        mainLayout.setMargin(true);
        setCompositionRoot(mainLayout);
    }

    @Override
    public void setConfiguration(CatalogConfig_V1 conf)
            throws DPUConfigException {
        catalogApiLocation.setValue(conf.getCatalogApiLocation());
    }

    @Override
    public CatalogConfig_V1 getConfiguration()
            throws DPUConfigException {
        CatalogConfig_V1 conf = new CatalogConfig_V1();
        conf.setCatalogApiLocation(catalogApiLocation.getValue());
        return conf;
    }

    @Override
    public String getDescription() {
        return catalogApiLocation.getValue();
    }
}
