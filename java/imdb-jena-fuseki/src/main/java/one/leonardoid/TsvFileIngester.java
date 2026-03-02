package one.leonardoid;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.NamedCsvRecord;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphMemFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import java.io.*;
import java.nio.file.Path;
import java.util.Map;

public class TsvFileIngester {

    private final Map<Path, ImdbToJena> pathJenaMap;

    static private final long BATCH_SIZE = 100_000;

    public TsvFileIngester(Map<Path, ImdbToJena> pathJenaMap) {
        this.pathJenaMap = pathJenaMap;
    }



    public void ingestAllNT(String outFilename) throws IOException {
        Graph gr = GraphMemFactory.createGraphMem2();
        try(OutputStream out = new BufferedOutputStream(new FileOutputStream(outFilename))) {
            for (Map.Entry<Path, ImdbToJena> e : pathJenaMap.entrySet()) {
                System.out.println("Processing ".concat(e.getKey().toString()));
                try (CsvReader<NamedCsvRecord> cr = GzipFileReader.openGzipFile( e.getKey().toString(), e.getValue().getEncoding())) {
                    cr.forEach(rec -> {
                        e.getValue().rowToNT(rec, gr);
                        if(gr.size() >= BATCH_SIZE) {
                            RDFDataMgr.write(out, gr, Lang.NTRIPLES);
                            gr.clear();
                        }
                    });
                    RDFDataMgr.write(out, gr, Lang.NTRIPLES);
                    gr.clear();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
