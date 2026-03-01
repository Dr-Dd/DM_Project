package one.leonardoid;

import de.siegmar.fastcsv.reader.NamedCsvRecord;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;

import java.util.Map;


public class TitleBasicsImdbToJena extends ImdbToJena {

    private static final String imdbTitleId = "https://imdb.com/title/";
    private static final String schema = "https://schema.org/";

    private static final Map<String, String> TYPE_TO_RDF = Map.ofEntries(
            Map.entry( "movie", "Movie"),
            Map.entry("short", "Movie"),
            Map.entry("tvEpisode","TVEpisode"),
            Map.entry("tvMiniSeries","TVSeries"),
            Map.entry("tvMovie","Movie"),
            Map.entry("tvPilot","TVEpisode"),
            Map.entry("tvSeries","TVSeries"),
            Map.entry("tvShort","Movie"),
            Map.entry("tvSpecial","Movie"),
            Map.entry("video","Movie"),
            Map.entry("videoGame","VideoGame")
    );

    private Model movieModel;
    private Property name;
    private Property alternateName;
    private Property isFamilyFriendly;
    private Property datePublished;
    private Property genre;

    public TitleBasicsImdbToJena(String encoding) {
        super(encoding);
    }

    @Override
    public void prepareModel(Dataset dataset) {
        System.out.println("Preparing TitleBasicsImdbToJena...");
        this.movieModel = dataset.getNamedModel("movie");
        this.name = this.movieModel.createProperty(schema, "name");
        this.alternateName = this.movieModel.createProperty(schema, "alternateName");
        this.isFamilyFriendly = this.movieModel.createProperty(schema, "isFamilyFriendly");
        this.datePublished = this.movieModel.createProperty(schema, "datePublished");
        this.genre = this.movieModel.createProperty(schema, "genre");
        System.out.println("TitleBasicsImdbToJena has been prepared.");
    }

/*
    public void writeNtriple(NamedCsvRecord rec, BufferedWriter writer) {
        try {
            // ... same field extraction as before ...
            String uri = imdbTitleId + tconst;
            // Write each triple as: <uri> <predicate> "object"^^datatype .
            // Use NtripleUtils or manual escaping.
            writer.write("<" + uri + "> <" + schema + "name> \"" + escape(primaryTitle) + "\" .\n");
            // ... etc.
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

*/
    @Override
    public void ingestRow(NamedCsvRecord rec) {
        try {
            String tconst = rec.getField("tconst");
            String titleType = rec.getField("titleType");
            String primaryTitle = rec.getField("primaryTitle");
            String originalTitle = rec.getField("originalTitle");
            String isAdult = rec.getField("isAdult");
            String startYear = rec.getField("startYear");
            String[] genres = rec.getField("genres").split(",");

            String uri = imdbTitleId.concat(tconst);
            Resource r = movieModel.createResource(uri);

            if (!titleType.equals("\\N")) {
                r.addProperty(RDF.type, schema.concat(TYPE_TO_RDF.get(titleType)));
            }
            if (!primaryTitle.equals("\\N")) {
                r.addProperty(name, primaryTitle);
            }
            if (!originalTitle.equals("\\N")) {
                r.addProperty(alternateName, originalTitle);
            }
            if (!isAdult.equals("\\N")) {
                r.addProperty(isFamilyFriendly, String.valueOf(isAdult.equals("0")), XSDDatatype.XSDboolean);
            }
            if (!startYear.equals("\\N")) {
                r.addProperty(datePublished, startYear, XSDDatatype.XSDgYear);
            }
            if (!genres[0].equals("\\N"))
                for (String g : genres) {
                    r.addProperty(genre, g);
                }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
