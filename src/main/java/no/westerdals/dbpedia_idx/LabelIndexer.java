package no.westerdals.dbpedia_idx;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LabelIndexer {
    final Pattern pattern = Pattern.compile("\\s*?<http://dbpedia.org/resource/(.*?)>\\s+<http://www.w3.org/2000/01/rdf-schema#(.*?)>\\s+\"(.*?)\".*");
    private final String inputFile;
    private final ElasticClient elasticClient;

    public LabelIndexer(final String inputFile) {
        this(inputFile, ElasticClient.createDefaultClientForIndex("labels"));
    }

    public LabelIndexer(final String inputFile, ElasticClient elasticClient) {
        this.inputFile = inputFile;
        this.elasticClient = elasticClient;
    }

    public void importLabelsToElastic() throws Exception {
        System.out.println("importing labels to Elastic..." + Runtime.getRuntime().totalMemory());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)))) {
            String line = null;
            int counter = 0;
            while ((line = reader.readLine()) != null) {
                counter = processLine(line, counter);
            }
            System.out.println("parsed::" + counter);
        }
        elasticClient.shutdown();
    }

    public int processLine(String line, int counter) {
        if (isCommentLine(line)) {
            return counter;
        }
        counter = incrementAndPrint(counter);
        final Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
            final Triple Triple = new Triple(matcher.group(1), matcher.group(2).toLowerCase(), matcher.group(3).toLowerCase());
            elasticClient.insertTripleLabel(Triple);
        }
        return counter;
    }

    private int incrementAndPrint(int counter) {
        if (counter++ % 10_000L == 0) {
            System.out.print(counter);
            System.out.print("\r");
        }
        return counter;
    }

    private boolean isCommentLine(String line) {
        return line.charAt(0) == '#';
    }


    public static void main(String[] args) throws Exception {
        if (args == null || args.length <= 0) {
            System.out.println("Usage: " + LabelIndexer.class.getName() + " <path_to_nt_file>");
            System.exit(1);
        }
        final String input = args[args.length - 1];
        final LabelIndexer labelsImporter = new LabelIndexer(input);
        labelsImporter.importLabelsToElastic();
    }

}
