package one.leonardoid;

import de.siegmar.fastcsv.reader.NamedCsvRecord;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import java.util.Iterator;

public abstract class ImdbToJena {

    private final String encoding;

    public ImdbToJena(String encoding) {
        this.encoding = encoding;
    }

    public String getEncoding() {
        return encoding;
    }

    public abstract void prepareModel(Dataset dataset);

    public abstract Iterator<Triple> rowToNT(NamedCsvRecord rec);

    public abstract void ingestRow(NamedCsvRecord rec);
}
