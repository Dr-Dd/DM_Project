package one.leonardoid;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.NamedCsvRecord;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.TDB2Factory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TsvFileIngester {

    private final Map<Path, ImdbToJena> pathJenaMap;

    static private final long BATCH_SIZE = 100_000;

    public TsvFileIngester(Map<Path, ImdbToJena> pathJenaMap) {
        this.pathJenaMap = pathJenaMap;
    }

    public void ingestAll(String outFilename) throws IOException {
        System.out.println("Creating dataset.");
        Dataset dataset = TDB2Factory.connectDataset(outFilename);
        System.out.println("Starting ingest.");
        for (Map.Entry<Path, ImdbToJena> e : pathJenaMap.entrySet()) {
            System.out.println("Processing ".concat(e.getKey().toString()));
            try (CsvReader<NamedCsvRecord> cr = GzipFileReader.openGzipFile(e.getKey().toString(), e.getValue().getEncoding())) {
                e.getValue().prepareModel(dataset);
                List<NamedCsvRecord> batch = new ArrayList<>();
                cr.forEach(rec -> {
                    batch.add(rec);
                    if (batch.size() >= BATCH_SIZE) {
                        System.out.println("Batch filled.");
                        this.processBatch(dataset,e.getValue(),batch);
                        batch.clear();
                    }
                });
                if(!batch.isEmpty())
                    this.processBatch(dataset,e.getValue(),batch);
            }
        }
        dataset.close();
    }

    private void processBatch(Dataset dataset, ImdbToJena converter, List<NamedCsvRecord> batch) {
        dataset.executeWrite(() -> {
            for (NamedCsvRecord rec : batch) {
                converter.ingestRow(rec);
            }
        });
    }
}
