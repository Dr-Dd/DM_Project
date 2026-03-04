package one.leonardoid;

import de.siegmar.fastcsv.reader.NamedCsvRecord;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.vocabulary.SchemaDO;

public class TitleAkasImdbToJena extends ImdbToJena {

    @Override
    public void rowToNT(NamedCsvRecord rec, StreamRDF srdf) {
        try {

            String isOriginalTitle = rec.getField("isOriginalTitle");

            if(!isOriginalTitle.equals("1")) {
                String titleId = rec.getField("titleId");
                String title = rec.getField("title");

                Node r = NodeFactory.createURI(ImdbSchema.title.getURI() + "/" + titleId);

                if(!title.equals("\\N"))
                    srdf.triple(Triple.create(r, SchemaDO.alternateName.asNode(), NodeFactory.createLiteralByValue(title)));
            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
