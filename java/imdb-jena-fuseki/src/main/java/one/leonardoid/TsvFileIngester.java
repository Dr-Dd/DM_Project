package one.leonardoid;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.NamedCsvRecord;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;

import java.io.*;
import java.nio.file.Path;
import java.util.Map;

public class TsvFileIngester {

    private final Map<Path, ImdbToJena> pathJenaMap;

    public TsvFileIngester(Map<Path, ImdbToJena> pathJenaMap) {
        this.pathJenaMap = pathJenaMap;
    }



    public void ingestAllNT(String outFilename) throws IOException {
        try(OutputStream out = new BufferedOutputStream(new FileOutputStream(outFilename))) {
            StreamRDF srdf = StreamRDFWriter.getWriterStream(out, Lang.NTRIPLES);
            srdf.start();
            for (Map.Entry<Path, ImdbToJena> e : pathJenaMap.entrySet()) {
                System.out.println("Processing ".concat(e.getKey().toString()));
                try (CsvReader<NamedCsvRecord> cr = GzipFileReader.openGzipFile( e.getKey().toString(), e.getValue().getEncoding())) {
                    cr.forEach(rec -> {
                        e.getValue().rowToNT(rec, srdf);
                    });
                }
            }
            srdf.finish();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
