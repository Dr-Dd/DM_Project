package one.leonardoid;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.NamedCsvRecord;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;

import java.io.*;
import java.nio.file.Path;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

public class TsvFileIngester {

    private final Map<Path, ImdbToJena> pathJenaMap;

    public TsvFileIngester(Map<Path, ImdbToJena> pathJenaMap) {
        this.pathJenaMap = pathJenaMap;
    }

    private static Path convertTsvGzToNtGz(Path file) {
        String filename = file.toString();
        if (!filename.endsWith(".tsv.gz")) {
            throw new IllegalArgumentException("Expected .tsv.gz, got: " + filename);
        }
        return Path.of(filename.substring(0, filename.length() - 7) + ".nt.gz");
    }


    public void ingestAllNT() throws IOException {
        for (Map.Entry<Path, ImdbToJena> e : pathJenaMap.entrySet()) {
            try(FileOutputStream fos = new FileOutputStream(convertTsvGzToNtGz(e.getKey()).toString());
                GZIPOutputStream gzos = new GZIPOutputStream(fos, 1024 * 64);
                BufferedOutputStream out = new BufferedOutputStream(gzos, 1024 * 64)) {
                StreamRDF srdf = StreamRDFWriter.getWriterStream(out, Lang.NTRIPLES);
                System.out.println("Processing ".concat(e.getKey().toString()).concat("."));
                srdf.start();
                try (CsvReader<NamedCsvRecord> cr = GzipFileReader.openGzipFile(e.getKey().toString())) {
                    cr.forEach(rec ->
                        e.getValue().rowToNT(rec, srdf)
                    );
                } finally {
                    srdf.finish();
                }
            }
        }
    }
}
