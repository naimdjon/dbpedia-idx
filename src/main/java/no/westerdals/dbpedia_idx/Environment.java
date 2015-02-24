package no.westerdals.dbpedia_idx;

import java.io.File;

public class Environment {

    public static boolean isLinux() {
        return System.getProperty("os.name").contains("Linux");
    }

    public static String getIndexDir() {
        String indexDir="/Users/takhirov/NEEL_LUCENE_INDEX";
        if (Environment.isLinux()) {
            indexDir=indexDir.replaceFirst("Users","home").replaceFirst("takhirov","taknai");
        }
        File f=new File(indexDir);
        if (!f.exists()) {
            f.mkdir();
        }
        return indexDir;
    }

    public static String getInputFile(final String name) {
        String inputFile="/Users/takhirov/Downloads/"+name+"_en.nt";
        if (Environment.isLinux()) {
            inputFile="/home/taknai/Downloads/"+name+"_en.nt";
        }
        return inputFile;
    }
}
