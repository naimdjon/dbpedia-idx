package no.westerdals.dbpedia_idx;

public class Environment {

    public static boolean isLinux() {
        return System.getProperty("os.name").contains("Linux");
    }

}
