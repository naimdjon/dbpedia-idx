package no.westerdals.dbpedia_idx;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static no.westerdals.dbpedia_idx.ElasticClient.createDefaultClientForIndex;

public class ArticleCategoriesIndexer {
    private final Pattern pattern = Pattern.compile("\\s*?<http://dbpedia.org/resource/(.*?)>\\s+<http://purl.org/dc/terms/(\\w+?)>\\s+<http://dbpedia.org/resource/Category:(.*?)>.*");
    private final String inputFile;
    private final ElasticClient elasticClient;
    public final TripleWithCategoryInserter inserter;
    private final AtomicInteger counter = new AtomicInteger();

    public ArticleCategoriesIndexer(final String inputFile) {
        this(inputFile, createDefaultClientForIndex("articlecategories"));
    }

    public ArticleCategoriesIndexer(final String inputFile, ElasticClient elasticClient) {
        this.inputFile = inputFile;
        this.elasticClient = elasticClient;
        this.inserter = new TripleWithCategoryInserter(this.elasticClient);
    }

    public void importToElastic() throws Exception {
        System.out.println("importing to Elastic..." + Runtime.getRuntime().totalMemory());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                processLine(line);
            }
            System.out.println("parsed::" + counter.get());
        }
        finishBatch();
        elasticClient.shutdown();
    }

    public int finishBatch() {
        return inserter.finishBatch();
    }

    public int processLine(String line) {
        if (isCommentLine(line)) {
            return counter.get();
        }
        incrementAndPrint();
        final Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
            final Triple Triple = new Triple(matcher.group(1), matcher.group(2).toLowerCase(), matcher.group(3).toLowerCase());
            inserter.insertTripleWithCategories(Triple);
        } else {
            System.err.println("not matched.");
        }
        return counter.get();
    }

    private void incrementAndPrint() {
        if (counter.incrementAndGet() % 10_000L == 0) {
            System.out.print(counter.get());
            System.out.print("\r");
        }
    }

    private boolean isCommentLine(String line) {
        return line.charAt(0) == '#';
    }


    public static void main(String[] args) throws Exception {
        if (args == null || args.length <= 0) {
            System.out.println("Usage: " + ArticleCategoriesIndexer.class.getName() + " <path_to_nt_file>");
            System.exit(1);
        }
        final String input = args[args.length - 1];
        final ArticleCategoriesIndexer importer = new ArticleCategoriesIndexer(input);
        importer.importToElastic();
        importer.check();
    }

    private void check() throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)))) {
            String line = null;
            Set<String> total = new HashSet<>();
            while ((line = reader.readLine()) != null) {
                final Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    total.add(matcher.group(1));
                } else {
                    System.err.println("not a matching line:" + line);
                }
            }
            System.out.println("Impoted:" + total.size());
        }
    }


}
