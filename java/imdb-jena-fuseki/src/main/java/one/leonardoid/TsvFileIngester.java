package one.leonardoid;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;

public class TsvFileIngester {

    public HashMap<Path, ImdbToJena> pathJenaMap;

    public TsvFileIngester(HashMap<Path, ImdbToJena> pathJenaMap) {
        this.pathJenaMap = pathJenaMap;
    }

    public HashMap<Path, ImdbToJena> getPathJenaMap() {
        return pathJenaMap;
    }

    public void setPathJenaMap(HashMap<Path, ImdbToJena> pathJenaMap) {
        this.pathJenaMap = pathJenaMap;
    }

    public void ingestAll() throws IOException {
        GzipFileReader r = new GzipFileReader();
        BufferedReader br;
        for (HashMap.Entry<Path, ImdbToJena> e : pathJenaMap.entrySet()) {
            br = r.OpenGzipFile(e.getKey().toString(), e.getValue().getEncoding());
        }
    }

}
