package one.leonardoid;

import de.siegmar.fastcsv.reader.NamedCsvRecord;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.vocabulary.SchemaDO;

public class TitleCrewImdbToJena extends ImdbToJena {

    @Override
    public void rowToNT(NamedCsvRecord rec, StreamRDF srdf) {
        try {
            String tconst = rec.getField("tconst");
            String directors = rec.getField("directors");
            String writers = rec.getField("writers");

            Node r = NodeFactory.createURI(ImdbSchema.title.getURI() + "/" + tconst);

            if(!directors.equals("\\N"))
                for(String director  : directors.split(",")) {
                    Node directorNode = NodeFactory.createURI(ImdbSchema.name.getURI() + "/" + director);
                    srdf.triple(Triple.create(r, SchemaDO.director.asNode(), directorNode));
                }
            if(!writers.equals("\\N"))
                for(String writer  : writers.split(",")) {
                    Node writerNode = NodeFactory.createURI(ImdbSchema.name.getURI() + "/" + writer);
                    srdf.triple(Triple.create(r, SchemaDO.author.asNode(), writerNode));
                }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
