package one.leonardoid;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class GzipFileReader {

    public GzipFileReader() { }

    public BufferedReader OpenGzipFile(String filePath, String encoding) throws IOException {
        try (InputStream inputStream = new FileInputStream(filePath);
             GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
             InputStreamReader inputStreamReader = new InputStreamReader(gzipInputStream, encoding);
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {

            return bufferedReader;
        }
    }
}
