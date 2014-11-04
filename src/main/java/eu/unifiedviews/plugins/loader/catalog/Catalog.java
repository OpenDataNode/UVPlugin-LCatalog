package eu.unifiedviews.plugins.loader.catalog;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.unifiedviews.dataunit.DataUnit;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.files.FilesDataUnit;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import eu.unifiedviews.dpu.DPU;
import eu.unifiedviews.dpu.DPUContext;
import eu.unifiedviews.dpu.DPUException;
import eu.unifiedviews.helpers.dataunit.resourcehelper.ResourceHelper;
import eu.unifiedviews.helpers.dataunit.resourcehelper.ResourceHelpers;
import eu.unifiedviews.helpers.dpu.config.AbstractConfigDialog;
import eu.unifiedviews.helpers.dpu.config.ConfigDialogProvider;
import eu.unifiedviews.helpers.dpu.config.ConfigurableBase;

@DPU.AsLoader
public class Catalog extends
        ConfigurableBase<CatalogConfig_V1> implements
        ConfigDialogProvider<CatalogConfig_V1> {
    private static final Logger LOG = LoggerFactory
            .getLogger(Catalog.class);

    @DataUnit.AsInput(name = "filesInput", optional = true)
    public FilesDataUnit filesInput;

    @DataUnit.AsInput(name = "rdfInput", optional = true)
    public RDFDataUnit rdfInput;

    public Catalog() {
        super(CatalogConfig_V1.class);
    }

    @Override
    public void execute(DPUContext dpuContext) throws DPUException,
            InterruptedException {
        String shortMessage = this.getClass().getSimpleName() + " starting.";
        String longMessage = String.valueOf(config);
        dpuContext.sendMessage(DPUContext.MessageType.INFO, shortMessage, longMessage);

        FilesDataUnit.Iteration filesIteration;
        try {
            filesIteration = filesInput.getIteration();
        } catch (DataUnitException ex) {
            throw new DPUException("Could not obtain filesInput", ex);
        }

        boolean shouldContinue = !dpuContext.canceled();
        ResourceHelper resourceHelper = ResourceHelpers.create(filesInput);
        try {
            StringBuilder sb = new StringBuilder("[");
            while (shouldContinue && filesIteration.hasNext()) {

                FilesDataUnit.Entry entry;
                entry = filesIteration.next();
                String symbolicName = entry.getSymbolicName();
                Map<String, String> resource = resourceHelper.getResource(symbolicName);
                String resourceUri = resource.get("uri");
                sb.append("{ \"uri\": \"");
                sb.append(resourceUri);
                sb.append("\", \"name\": \"");
                sb.append(symbolicName);
                sb.append("\" },");

                shouldContinue = !dpuContext.canceled();
            }
            sb.delete(sb.length() - 1, sb.length());
            sb.append("]");
            LOG.info("Request: " + sb.toString());
            CloseableHttpClient client = HttpClients.createDefault();
            URIBuilder uriBuilder = new URIBuilder(config.getCatalogApiLocation());
            uriBuilder.setPath(uriBuilder.getPath() + '/' + config.getDatasetId());
            HttpPost httpPost = new HttpPost(uriBuilder.build().normalize());
            httpPost.addHeader(new BasicHeader("Content-Type", "application/json"));
            httpPost.setEntity(new StringEntity(sb.toString(), Charset.forName("utf-8")));
            CloseableHttpResponse response = null;
            try {
                response = client.execute(httpPost);
                if (response.getStatusLine().getStatusCode() == 200) {
                    LOG.info("Response:" + EntityUtils.toString(response.getEntity()));
                } else {
                    LOG.error("Response:" + EntityUtils.toString(response.getEntity()));
                }
            } catch (IOException ex) {
                throw new DPUException(ex);
            } finally {
                if (response != null) {
                    response.close();
                }
            }
        } catch (DataUnitException ex) {
            throw new DPUException("Error iterating filesInput.", ex);
        } catch (IOException ex) {
            throw new DPUException("Error in http", ex);
        } catch (URISyntaxException ex) {
            throw new DPUException("Error in http", ex);
        } finally {
            try {
                filesIteration.close();
            } catch (DataUnitException ex) {
                LOG.warn("Error closing filesInput", ex);
            }
            try {
                resourceHelper.close();
            } catch (DataUnitException ex) {
                LOG.warn("Error in close", ex);
            }
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
