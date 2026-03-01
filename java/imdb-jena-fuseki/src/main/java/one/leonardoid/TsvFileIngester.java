package one.leonardoid;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.NamedCsvRecord;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.tdb2.TDB2Factory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class TsvFileIngester {

    private final Map<Path, ImdbToJena> pathJenaMap;

    static private final long BATCH_SIZE = 100_000;

    public TsvFileIngester(Map<Path, ImdbToJena> pathJenaMap) {
        this.pathJenaMap = pathJenaMap;
    }

    public void ingestAll(String dir) throws IOException {
        Dataset dataset = TDB2Factory.connectDataset(dir);
        GzipFileReader r = new GzipFileReader();
        for (Map.Entry<Path, ImdbToJena> e : pathJenaMap.entrySet()) {
            try (CsvReader<NamedCsvRecord> cr = r.OpenGzipFile(e.getKey().toString(), e.getValue().getEncoding())) {
                e.getValue().prepareModel(dataset);
                dataset.begin(ReadWrite.WRITE);
                long batchCount = 0;
                try {
                    for (NamedCsvRecord rec : cr) {
                        e.getValue().ingestRow(rec);
                        batchCount++;
                        if (batchCount >= BATCH_SIZE) {
                            dataset.commit();
                            System.out.println("Batch committed");
                            dataset.end();
                            dataset.begin(ReadWrite.WRITE);
                            batchCount = 0;
                        }
                    }
                    dataset.commit();
                } catch (Exception ex) {
                    dataset.abort();
                    throw new RuntimeException(ex);
                } finally {
                    dataset.end();
                }
            }
        }
        dataset.close();
    }
}
