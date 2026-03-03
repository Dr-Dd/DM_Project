package one.leonardoid;

import de.siegmar.fastcsv.reader.NamedCsvRecord;
import org.apache.jena.riot.system.StreamRDF;

public abstract class ImdbToJena {

    public ImdbToJena() {}

    public abstract void rowToNT(NamedCsvRecord rec, StreamRDF srdf);
}
