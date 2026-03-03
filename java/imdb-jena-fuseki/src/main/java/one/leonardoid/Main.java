package one.leonardoid;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        TsvFileIngester tfi = new TsvFileIngester(Map.ofEntries(
                Map.entry(Path.of("/data", "title.basics.tsv.gz"), new TitleBasicsImdbToJena()),
                Map.entry(Path.of("/data", "name.basics.tsv.gz"), new NameBasicsImdbToJena()),
                Map.entry(Path.of("/data", "title.akas.tsv.gz"), new TitleAkasImdbToJena()),
                Map.entry(Path.of("/data", "title.crew.tsv.gz"), new TitleCrewImdbToJena()),
                Map.entry(Path.of("/data", "title.ratings.tsv.gz"), new TitleRatingsImdbToJena())
        ));
        try {
            System.out.println("Starting import.");
            tfi.ingestAllNT();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

