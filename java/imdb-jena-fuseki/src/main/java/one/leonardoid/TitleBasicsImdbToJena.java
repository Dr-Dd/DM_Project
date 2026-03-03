package one.leonardoid;

import de.siegmar.fastcsv.reader.NamedCsvRecord;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.SchemaDO;

import java.util.Map;


public class TitleBasicsImdbToJena extends ImdbToJena {

    private static final Map<String, Node> TYPE_TO_RDF = Map.ofEntries(
            Map.entry("movie", SchemaDO.Movie.asNode()),
            Map.entry("short",SchemaDO.Movie.asNode()),
            Map.entry("tvEpisode",SchemaDO.TVEpisode.asNode()),
            Map.entry("tvMiniSeries",SchemaDO.TVSeries.asNode()),
            Map.entry("tvMovie",SchemaDO.Movie.asNode()),
            Map.entry("tvPilot",SchemaDO.TVEpisode.asNode()),
            Map.entry("tvSeries",SchemaDO.TVSeries.asNode()),
            Map.entry("tvShort",SchemaDO.Movie.asNode()),
            Map.entry("tvSpecial",SchemaDO.Movie.asNode()),
            Map.entry("video",SchemaDO.Movie.asNode()),
            Map.entry("videoGame",SchemaDO.VideoGame.asNode())
    );

    public TitleBasicsImdbToJena() {
        super();
    }

    @Override
    public void rowToNT(NamedCsvRecord rec, StreamRDF srdf) {
        try {
            String tconst = rec.getField("tconst");
            String titleType = rec.getField("titleType");
            String primaryTitle = rec.getField("primaryTitle");
            String originalTitle = rec.getField("originalTitle");
            String isAdult = rec.getField("isAdult");
            String startYear = rec.getField("startYear");
            String genres = rec.getField("genres");

            Node r = NodeFactory.createURI(ImdbSchema.title.getURI().concat(tconst));

            if (!titleType.equals("\\N")) {
                srdf.triple(Triple.create(r,RDF.type.asNode(), TYPE_TO_RDF.get(titleType)));
            }
            if (!primaryTitle.equals("\\N")) {
                srdf.triple(Triple.create(r, SchemaDO.name.asNode(), NodeFactory.createLiteralByValue(primaryTitle)));
            }
            if (!originalTitle.equals("\\N")) {
                srdf.triple(Triple.create(r, SchemaDO.alternateName.asNode(), NodeFactory.createLiteralByValue(originalTitle)));
            }
            if (!isAdult.equals("\\N")) {
                srdf.triple(Triple.create(r, SchemaDO.isFamilyFriendly.asNode(), NodeFactory.createLiteralByValue(isAdult.equals("0"))));
            }
            if (!startYear.equals("\\N")) {
                srdf.triple(Triple.create(r, SchemaDO.datePublished.asNode(), NodeFactory.createLiteralByValue(startYear, XSDDatatype.XSDgYear)));
            }
            if (!genres.equals("\\N"))
                for (String g : genres.split(","))
                    srdf.triple(Triple.create(r, SchemaDO.genre.asNode(), NodeFactory.createLiteralByValue(g)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
