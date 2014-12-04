package eu.unifiedviews.plugins.loader.catalog;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

public class DCAT {
    /** http://www.w3.org/ns/dcat# */
    public static final String NAMESPACE = "http://www.w3.org/ns/dcat#";

    /**
    * Recommended prefix for the RDF namespace: "rdf"
    */
    public static final String PREFIX = "dcat";

    /**
    * An immutable {@link Namespace} constant that represents the http://www.w3.org/ns/dcat# namespace.
    */
    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    /** http://www.w3.org/ns/dcat#Distribution */
    public final static URI Distribution;

    /** http://www.w3.org/ns/dcat#distribution */
    public final static URI distribution;

    /** http://www.w3.org/ns/dcat#downloadURL */
    public final static URI downloadURL;

    /** http://www.w3.org/ns/dcat#accessURL */
    public final static URI accessURL;

    /** http://www.w3.org/ns/dcat#Distribution */
    public final static URI Dataset;

    static {
        ValueFactory factory = ValueFactoryImpl.getInstance();
        Distribution = factory.createURI(NAMESPACE, "Distribution");
        distribution = factory.createURI(NAMESPACE, "distribution");
        downloadURL = factory.createURI(NAMESPACE, "downloadURL");
        accessURL = factory.createURI(NAMESPACE, "accessURL");
        Dataset = factory.createURI(NAMESPACE, "Dataset");
    }
}
