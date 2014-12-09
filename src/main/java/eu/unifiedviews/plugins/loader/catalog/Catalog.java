package eu.unifiedviews.plugins.loader.catalog;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.openrdf.rio.UnsupportedRDFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.unifiedviews.dataunit.DataUnit;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.files.FilesDataUnit;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import eu.unifiedviews.dpu.DPU;
import eu.unifiedviews.dpu.DPUContext;
import eu.unifiedviews.dpu.DPUException;
import eu.unifiedviews.helpers.dataunit.fileshelper.FilesHelper;
import eu.unifiedviews.helpers.dataunit.virtualpathhelper.VirtualPathHelpers;
import eu.unifiedviews.helpers.dpu.config.AbstractConfigDialog;
import eu.unifiedviews.helpers.dpu.config.ConfigDialogProvider;
import eu.unifiedviews.helpers.dpu.config.ConfigurableBase;

@DPU.AsLoader
public class Catalog extends ConfigurableBase<CatalogConfig_V1> implements ConfigDialogProvider<CatalogConfig_V1> {
    private static final Logger LOG = LoggerFactory.getLogger(Catalog.class);

    @DataUnit.AsInput(name = "filesInput", optional = true)
    public FilesDataUnit filesInput;

    @DataUnit.AsInput(name = "rdfInput", optional = true)
    public RDFDataUnit rdfInput;

    public Catalog() {
        super(CatalogConfig_V1.class);
    }

    @Override
    public void execute(DPUContext dpuContext) throws DPUException, InterruptedException {
        String shortMessage = this.getClass().getSimpleName() + " starting.";
        String longMessage = String.valueOf(config);
        dpuContext.sendMessage(DPUContext.MessageType.INFO, shortMessage, longMessage);

        if (rdfInput == null && filesInput == null) {
            throw new DPUException("No input data unit for me, exiting");
        }

        if (filesInput != null) {
            CloseableHttpResponse response = null;
            try {
                Set<FilesDataUnit.Entry> files = FilesHelper.getFiles(filesInput);
                StringBuilder sb = new StringBuilder("{");
                sb.append("\"pipelineId\": 307, \"resources\": [");
                for (FilesDataUnit.Entry file : files) {
                    sb.append("{ \"storageId\": { \"type\": \"FILE\", \"value\": \"");
                    String storageId = VirtualPathHelpers.getVirtualPath(filesInput, file.getSymbolicName());
                    if (storageId == null || storageId.isEmpty()) {
                        storageId = file.getSymbolicName();
                    }
                    sb.append(storageId);
                    sb.append("\" }, \"resource\": { \"name\": \"");
                    sb.append(storageId);
                    sb.append("\" } },");
                }
                sb.delete(sb.length() - 1, sb.length());

                LOG.info("Request (json): " + sb.toString());

                CloseableHttpClient client = HttpClients.createDefault();
                URIBuilder uriBuilder = new URIBuilder(config.getCatalogApiLocation());
                uriBuilder.setPath(uriBuilder.getPath());
                HttpPost httpPost = new HttpPost(uriBuilder.build().normalize());
                HttpEntity entity = EntityBuilder.create()
                        .setText(sb.toString())
                        .setContentType(ContentType.APPLICATION_JSON.withCharset(Charset.forName("utf-8")))
                        .build();
                httpPost.setEntity(entity);
                response = client.execute(httpPost);
                if (response.getStatusLine().getStatusCode() == 200) {
                    LOG.info("Response:" + EntityUtils.toString(response.getEntity()));
                } else {
                    LOG.error("Response:" + EntityUtils.toString(response.getEntity()));
                }
            } catch (UnsupportedRDFormatException | DataUnitException | IOException | URISyntaxException ex) {
                throw new DPUException("Error exporting metadata", ex);
            } finally {
                if (response != null) {
                    try {
                        response.close();
                    } catch (IOException ex) {
                        LOG.warn("Error in close", ex);
                    }
                }
            }
        }
        if (rdfInput != null) {
        }
    }

    @Override
    public AbstractConfigDialog<CatalogConfig_V1> getConfigurationDialog() {
        return new CatalogVaadinDialog();
    }

    public static String appendNumber(long number) {
        String value = String.valueOf(number);
        if (value.length() > 1) {
            // Check for special case: 11 - 13 are all "th".
            // So if the second to last digit is 1, it is "th".
            char secondToLastDigit = value.charAt(value.length() - 2);
            if (secondToLastDigit == '1') {
                return value + "th";
            }
        }
        char lastDigit = value.charAt(value.length() - 1);
        switch (lastDigit) {
            case '1':
                return value + "st";
            case '2':
                return value + "nd";
            case '3':
                return value + "rd";
            default:
                return value + "th";
        }
    }
}
