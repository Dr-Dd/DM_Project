package one.leonardoid;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;

public class ImdbSchema {
    private static final Model m = ModelFactory.createDefaultModel();
    public static final String NS = "https://www.imdb.com/";
    public static final Resource title;
    public static final Resource name;

    static {
        title = m.createResource("https://www.imdb.com/title");
        name = m.createResource("https://www.imdb.com/name");
    }
}
