package one.leonardoid;

import de.siegmar.fastcsv.reader.NamedCsvRecord;
import org.apache.jena.graph.Graph;

public abstract class ImdbToJena {

    private final String encoding;

    public ImdbToJena(String encoding) {
        this.encoding = encoding;
    }

    public String getEncoding() {
        return encoding;
    }

    public abstract void rowToNT(NamedCsvRecord rec, Graph gr);
}
