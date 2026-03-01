package one.leonardoid;

import de.siegmar.fastcsv.reader.NamedCsvRecord;
import org.apache.jena.query.Dataset;

public abstract class ImdbToJena {

    public String encoding;

    public ImdbToJena(String encoding) {
        this.encoding = encoding;
    }

    public String getEncoding() {
        return encoding;
    }

    public abstract void prepareModel(Dataset dataset);

    public abstract void ingestRow(NamedCsvRecord rec);
}
