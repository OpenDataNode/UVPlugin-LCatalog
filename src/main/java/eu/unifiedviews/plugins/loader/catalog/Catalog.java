package eu.unifiedviews.plugins.loader.catalog;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.openrdf.model.BNode;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.DCTERMS;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.Rio;
import org.openrdf.rio.UnsupportedRDFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.unifiedviews.dataunit.DataUnit;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.MetadataDataUnit;
import eu.unifiedviews.dataunit.WritableMetadataDataUnit;
import eu.unifiedviews.dataunit.files.FilesDataUnit;
import eu.unifiedviews.dataunit.files.WritableFilesDataUnit;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import eu.unifiedviews.dpu.DPU;
import eu.unifiedviews.dpu.DPUContext;
import eu.unifiedviews.dpu.DPUException;
import eu.unifiedviews.helpers.dataunit.copyhelper.CopyHelper;
import eu.unifiedviews.helpers.dataunit.copyhelper.CopyHelpers;
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

    @DataUnit.AsOutput(name = "filesOutput", optional = true)
    public WritableFilesDataUnit filesOutput;

    @DataUnit.AsOutput(name = "rdfOutput", optional = true)
    public WritableRDFDataUnit rdfOutput;

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
            executeOneDataUnit(dpuContext, filesInput, filesOutput);
        }
        if (rdfInput != null) {
            executeOneDataUnit(dpuContext, rdfInput, rdfOutput);
        }
    }

    private void executeOneDataUnit(DPUContext dpuContext, MetadataDataUnit dataUnit, WritableMetadataDataUnit writableDataUnit) throws DPUException {
        CloseableHttpResponse response = null;
        ValueFactory valueFactory = null;
        MetadataDataUnit.Iteration iteration = null;
        RepositoryConnection outputMetadataConnection = null;
        CopyHelper copyHelper = CopyHelpers.create(dataUnit, writableDataUnit);
        try {
            iteration = dataUnit.getIteration();
            outputMetadataConnection = writableDataUnit.getConnection();
            valueFactory = outputMetadataConnection.getValueFactory();
            BNode datasetResource = valueFactory.createBNode();

            outputMetadataConnection.add(valueFactory.createStatement(
                    datasetResource, RDF.TYPE, DCAT.Dataset),
                    writableDataUnit.getMetadataWriteGraphname());

            while (iteration.hasNext()) {
                MetadataDataUnit.Entry entry = iteration.next();

                BNode symbolicNameResource = valueFactory.createBNode();

                outputMetadataConnection.add(valueFactory.createStatement(
                        datasetResource, DCAT.distribution, symbolicNameResource),
                        filesOutput.getMetadataWriteGraphname());

                outputMetadataConnection.add(valueFactory.createStatement(
                        symbolicNameResource, RDF.TYPE, DCAT.Distribution),
                        filesOutput.getMetadataWriteGraphname());

                outputMetadataConnection.add(valueFactory.createStatement(
                        symbolicNameResource, DCTERMS.TITLE, valueFactory.createLiteral(entry.getSymbolicName())),
                        filesOutput.getMetadataWriteGraphname());

                outputMetadataConnection.add(valueFactory.createStatement(
                        symbolicNameResource, valueFactory.createURI(MetadataDataUnit.PREDICATE_SYMBOLIC_NAME), valueFactory.createLiteral(entry.getSymbolicName())),
                        filesOutput.getMetadataWriteGraphname());

                copyHelper.copyMetadata(entry.getSymbolicName());
            }

            File rdfFile = Files.createTempFile(dpuContext.getWorkingDir().toPath(), "request", ".rdf").toFile();
            FileWriter writer = new FileWriter(rdfFile);
            try {
                outputMetadataConnection.export(Rio.createWriter(RDFFormat.TURTLE, writer), dataUnit.getMetadataGraphnames().toArray(new URIImpl[0]));
            } finally {
                writer.close();
            }

            StringBuilder sb = new StringBuilder("{\"pipelineId\": 15 }");
            LOG.info("Request (json): " + sb.toString());
            LOG.info("Request (rdfFile): " + FileUtils.readFileToString(rdfFile, Charset.forName("utf-8")));

            CloseableHttpClient client = HttpClients.createDefault();
            URIBuilder uriBuilder = new URIBuilder(config.getCatalogApiLocation());
            uriBuilder.setPath(uriBuilder.getPath());
            HttpPost httpPost = new HttpPost(uriBuilder.build().normalize());
            HttpEntity entity = MultipartEntityBuilder.create()
                    .addTextBody("json", sb.toString(), ContentType.APPLICATION_JSON.withCharset(Charset.forName("utf-8")))
                    .addBinaryBody("rdf", rdfFile, ContentType.create("application/rdf+xml", Charset.forName("utf-8")), "metadata.rdf")
                    .build();
            httpPost.setEntity(entity);
            response = client.execute(httpPost);
            if (response.getStatusLine().getStatusCode() == 200) {
                LOG.info("Response:" + EntityUtils.toString(response.getEntity()));
            } else {
                LOG.error("Response:" + EntityUtils.toString(response.getEntity()));
            }
        } catch (RepositoryException | RDFHandlerException | UnsupportedRDFormatException | DataUnitException | IOException | URISyntaxException ex) {
            throw new DPUException("Error exporting metadata", ex);
        } finally {
            if (iteration != null) {
                try {
                    iteration.close();
                } catch (DataUnitException ex) {
                    LOG.warn("Error in close", ex);
                }
            }
            if (copyHelper != null) {
                try {
                    copyHelper.close();
                } catch (DataUnitException ex) {
                    LOG.warn("Error in close", ex);
                }
            }
            if (outputMetadataConnection != null) {
                try {
                    outputMetadataConnection.close();
                } catch (RepositoryException ex) {
                    LOG.warn("Error in close", ex);
                }
            }
            if (response != null) {
                try {
                    response.close();
                } catch (IOException ex) {
                    LOG.warn("Error in close", ex);
                }
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
