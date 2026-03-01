package one.leonardoid;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        TsvFileIngester tfi = new TsvFileIngester(Map.ofEntries(
                Map.entry(Path.of("/data", "title.basics.tsv.gz"), new TitleBasicsImdbToJena("UTF-8"))
        ));
        try {
            System.out.println("Starting import.");
            tfi.ingestAll(Path.of("/fusekidata", "imdb.dat").toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

