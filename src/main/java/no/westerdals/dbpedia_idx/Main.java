package no.westerdals.dbpedia_idx;

public class Main {

    public static void main(String[] args) throws Exception {
        String inputFile="/Users/takhirov/Downloads/labels_en.nt";
        if (Environment.isLinux()) {
            inputFile="/home/takhirov/Downloads/labels_en.nt";
        }
        new DBPediaLabelParser(inputFile).importLabels();
    }
}
