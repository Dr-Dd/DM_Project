package one.leonardoid;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.NamedCsvRecord;

import java.io.*;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;

public class GzipFileReader {

    public GzipFileReader() { }


    public CsvReader<NamedCsvRecord> OpenGzipFile(String filePath, String encoding) throws IOException {
        InputStream inputStream = new FileInputStream(filePath);
        try {
            GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
            Reader reader = new InputStreamReader(gzipInputStream, Charset.forName(encoding));
            return CsvReader.builder().fieldSeparator('\t').ofNamedCsvRecord(reader);
        } catch (IOException e) {
            inputStream.close();
            throw e;
        }
    }
}
