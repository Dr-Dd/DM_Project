package one.leonardoid;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.NamedCsvRecord;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class GzipFileReader {

    public GzipFileReader() { }

    public static CsvReader<NamedCsvRecord> openGzipFile(String filePath) {
        System.out.println("Opening gzip file ".concat(filePath).concat("."));
        try {
            FileInputStream fis = new FileInputStream(filePath);
            GZIPInputStream gzis = new GZIPInputStream(fis, 1024 * 64);
            BufferedInputStream in = new BufferedInputStream(gzis, 1024 * 64);
            return CsvReader.builder().fieldSeparator('\t').quoteCharacter('\0').ofNamedCsvRecord(in);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
