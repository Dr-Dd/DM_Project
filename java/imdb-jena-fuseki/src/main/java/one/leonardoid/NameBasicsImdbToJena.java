package one.leonardoid;

import de.siegmar.fastcsv.reader.NamedCsvRecord;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.SchemaDO;

public class NameBasicsImdbToJena extends ImdbToJena {

    public NameBasicsImdbToJena() {
        super();
    }

    @Override
    public void rowToNT(NamedCsvRecord rec, StreamRDF srdf) {
        try {
            String nconst = rec.getField("nconst");
            String primaryName = rec.getField("primaryName");
            String birthYear = rec.getField("birthYear");
            String deathYear = rec.getField("deathYear");
            String primaryProfessions = rec.getField("primaryProfession");
            String knownForTitles = rec.getField("knownForTitles");

            Node r = NodeFactory.createURI(ImdbSchema.name.getURI() + "/" + nconst);

            if(!nconst.equals("\\N"))
                srdf.triple(Triple.create(r, RDF.type.asNode(), SchemaDO.Person.asNode()));
            if(!primaryName.equals("\\N"))
                srdf.triple(Triple.create(r, SchemaDO.name.asNode(), NodeFactory.createLiteralByValue(primaryName)));
            if(!birthYear.equals("\\N"))
                srdf.triple(Triple.create(r, SchemaDO.birthDate.asNode(), NodeFactory.createLiteralByValue(birthYear, XSDDatatype.XSDgYear)));
            if(!deathYear.equals("\\N"))
                srdf.triple(Triple.create(r, SchemaDO.deathDate.asNode(), NodeFactory.createLiteralByValue(deathYear, XSDDatatype.XSDgYear)));
            if(!primaryProfessions.equals("\\N"))
                for(String profession : primaryProfessions.split(","))
                    srdf.triple(Triple.create(r, SchemaDO.jobTitle.asNode(), NodeFactory.createLiteralByValue(profession)));
            if(!knownForTitles.equals("\\N"))
                for(String title : knownForTitles.split(","))
                    srdf.triple(Triple.create( NodeFactory.createURI(ImdbSchema.title.getURI() + "/" + title), SchemaDO.actor.asNode(), r ));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
