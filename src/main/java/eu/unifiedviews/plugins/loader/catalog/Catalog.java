package eu.unifiedviews.plugins.loader.catalog;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObjectBuilder;

import eu.unifiedviews.helpers.dataunit.files.FilesHelper;
import eu.unifiedviews.helpers.dataunit.rdf.RDFHelper;
import eu.unifiedviews.helpers.dataunit.resource.Resource;
import eu.unifiedviews.helpers.dataunit.resource.ResourceConverter;
import eu.unifiedviews.helpers.dataunit.resource.ResourceHelpers;
import eu.unifiedviews.helpers.dataunit.virtualgraph.VirtualGraphHelpers;
import eu.unifiedviews.helpers.dataunit.virtualpath.VirtualPathHelpers;
import eu.unifiedviews.helpers.dpu.config.ConfigHistory;
import eu.unifiedviews.helpers.dpu.config.migration.ConfigurationUpdate;
import eu.unifiedviews.helpers.dpu.context.ContextUtils;
import eu.unifiedviews.helpers.dpu.exec.AbstractDpu;
import eu.unifiedviews.helpers.dpu.extension.ExtensionInitializer;
import eu.unifiedviews.helpers.dpu.extension.faulttolerance.FaultTolerance;
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

@DPU.AsLoader
public class Catalog extends AbstractDpu<CatalogConfig_V1> {
    private static final Logger LOG = LoggerFactory.getLogger(Catalog.class);

    @DataUnit.AsInput(name = "filesInput", optional = true)
    public FilesDataUnit filesInput;

    @DataUnit.AsInput(name = "rdfInput", optional = true)
    public RDFDataUnit rdfInput;

    @ExtensionInitializer.Init
    public FaultTolerance faultTolerance;

    @ExtensionInitializer.Init(param = "eu.unifiedviews.plugins.transformer.zipper.ZipperConfig__V1")
    public ConfigurationUpdate _ConfigurationUpdate;

    public Catalog() {
        super(CatalogVaadinDialog.class, ConfigHistory.noHistory(CatalogConfig_V1.class));
    }

    @Override
    protected void innerExecute() throws DPUException {
        String shortMessage = ctx.tr("catalog.starting");
        String longMessage = String.valueOf(config);
        ContextUtils.sendMessage(ctx, DPUContext.MessageType.INFO, shortMessage, longMessage);

        if (rdfInput == null && filesInput == null) {
            throw ContextUtils.dpuException(ctx, "catalog.error.no.input");
        }

        CloseableHttpResponse response = null;
        try {
            JsonBuilderFactory factory = Json.createBuilderFactory(Collections.<String, Object> emptyMap());
            JsonObjectBuilder rootBuilder = factory.createObjectBuilder();
            rootBuilder.add("pipelineId", ctx.getExecMasterContext().getDpuContext().getPipelineId());
            JsonArrayBuilder resourcesArray = factory.createArrayBuilder();

            if (filesInput != null) {
                Set<FilesDataUnit.Entry> files = FilesHelper.getFiles(filesInput);
                for (FilesDataUnit.Entry file : files) {
                    String storageId = VirtualPathHelpers.getVirtualPath(filesInput, file.getSymbolicName());
                    if (storageId == null || storageId.isEmpty()) {
                        storageId = file.getSymbolicName();
                    }
                    Resource resource = ResourceHelpers.getResource(filesInput, file.getSymbolicName());
                    resource.setName(storageId);
                    resourcesArray.add(factory.createObjectBuilder()
                            .add("storageId",
                                    factory.createObjectBuilder()
                                            .add("type", "FILE")
                                            .add("value", storageId))
                            .add("resource", buildResource(factory, resource, storageId))
                            );
                }
            }
            if (rdfInput != null) {
                Set<RDFDataUnit.Entry> graphs = RDFHelper.getGraphs(rdfInput);
                for (RDFDataUnit.Entry graph : graphs) {
                    String storageId = VirtualGraphHelpers.getVirtualGraph(rdfInput, graph.getSymbolicName());
                    if (storageId == null || storageId.isEmpty()) {
                        storageId = graph.getSymbolicName();
                    }
                    Resource resource = ResourceHelpers.getResource(rdfInput, graph.getSymbolicName());
                    resource.setName(storageId);
                    resourcesArray.add(factory.createObjectBuilder()
                            .add("storageId",
                                    factory.createObjectBuilder()
                                            .add("type", "RDF")
                                            .add("value", storageId))
                            .add("resource", buildResource(factory, resource, storageId))
                            );
                }
            }

            rootBuilder.add("resources", resourcesArray);
            String requestString = rootBuilder.build().toString();

            LOG.info("Request (json): " + requestString);

            CloseableHttpClient client = HttpClients.createDefault();
            URIBuilder uriBuilder = new URIBuilder(config.getCatalogApiLocation());
            uriBuilder.setPath(uriBuilder.getPath());
            HttpPost httpPost = new HttpPost(uriBuilder.build().normalize());
            HttpEntity entity = EntityBuilder.create()
                    .setText(requestString)
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
            throw ContextUtils.dpuException(ctx, ex, "catalog.error.metadata.export");
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

    private JsonObjectBuilder buildResource(JsonBuilderFactory factory, Resource resource, String storageId) {
        JsonObjectBuilder resourceExtrasBuilder = factory.createObjectBuilder();
        for (Map.Entry<String, String> mapEntry : ResourceConverter.extrasToMap(resource.getExtras()).entrySet()) {
            resourceExtrasBuilder.add(mapEntry.getKey(), mapEntry.getValue());
        }

        JsonObjectBuilder resourceBuilder = factory.createObjectBuilder();
        for (Map.Entry<String, String> mapEntry : ResourceConverter.resourceToMap(resource).entrySet()) {
            resourceBuilder.add(mapEntry.getKey(), mapEntry.getValue());
        }
        resourceBuilder.add("extras", resourceExtrasBuilder);

        return resourceBuilder;
    }
}
