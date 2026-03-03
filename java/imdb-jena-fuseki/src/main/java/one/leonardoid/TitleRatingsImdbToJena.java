package one.leonardoid;

import de.siegmar.fastcsv.reader.NamedCsvRecord;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.SchemaDO;

public class TitleRatingsImdbToJena extends ImdbToJena{
    @Override
    public void rowToNT(NamedCsvRecord rec, StreamRDF srdf) {
        try {
            String tconst = rec.getField("tconst");
            String averageRating = rec.getField("averageRating");
            String numVotes = rec.getField("numVotes");

            String baseURI = ImdbSchema.title.getURI() + "/" + tconst;
            Node r = NodeFactory.createURI(baseURI);

            Node ar = NodeFactory.createURI(baseURI + "/reviews");
            srdf.triple(Triple.create(ar, RDF.type.asNode(), SchemaDO.AggregateRating.asNode()));
            srdf.triple(Triple.create(r, SchemaDO.aggregateRating.asNode(), ar));
            srdf.triple(Triple.create(ar, SchemaDO.ratingCount.asNode(), NodeFactory.createLiteralByValue(numVotes)));
            srdf.triple(Triple.create(ar, SchemaDO.ratingValue.asNode(), NodeFactory.createLiteralByValue(averageRating)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
