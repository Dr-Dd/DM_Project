package one.leonardoid;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class Main {
    static void main() {
        TsvFileIngester tfi = new TsvFileIngester(Map.ofEntries(
                Map.entry(Path.of("data", "title.basics.tar.gz"), new TitleBasicsImdbToJena("UTF-8"))
        ));
        try {
            tfi.ingestAll(Path.of("data", "imdb.dat").toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

