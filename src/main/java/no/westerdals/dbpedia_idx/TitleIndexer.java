package no.westerdals.dbpedia_idx;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TitleIndexer {
    final Pattern labelPattern = Pattern.compile("\\s*?<http://dbpedia.org/resource/(.*?)>\\s+<http://www.w3.org/2000/01/rdf-schema#(.*?)>\\s+\"(.*?)\".*");
    private final String inputFile;

    public TitleIndexer(final String inputFile) {
        this.inputFile = inputFile;
    }

    public void parseLabel(final TripleProcessor tripleProcessor, final String content) throws Exception {
        Splitter.on(CharMatcher.anyOf("\n"))
                .omitEmptyStrings()
                .split(content)
                .forEach(labelConsumer(tripleProcessor));
    }

    private Consumer<String> labelConsumer(final TripleProcessor tripleProcessor) {
        return line -> {
            final Matcher matcher = labelPattern.matcher(line);
            if (matcher.find()) {
                tripleProcessor.process(new Triple(matcher.group(1).toLowerCase(), matcher.group(2).toLowerCase(), matcher.group(3).toLowerCase()));
            } else {
                System.err.println("No match for line " + line);
            }
        };
    }

    SolrBackend solrBackend = new SolrBackend();

    public void importLabelsToSolr() throws Exception {
        System.out.println("importing labels to Solr...");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)))) {
            final StringBuilder buffer = new StringBuilder();
            String line = null;
            int counter = 0;
            final AtomicLong totalCounter = new AtomicLong(0L);
            while ((line = reader.readLine()) != null) {
                if (line.charAt(0) == '#') continue;
                buffer.append(line).append("\n");
                if (++counter == 10_000L) {
                    parseLabel(triple -> {
                        totalCounter.incrementAndGet();
                        solrBackend.insertTripleLabel(triple);
                    }, buffer.toString());
                    System.out.format("%,8d", totalCounter.get());
                    System.out.print("\r");
                    counter = 0;
                    buffer.delete(0, buffer.length());
                }
            }
            parseLabel(triple -> {
                solrBackend.insertTripleLabel(triple);
                totalCounter.incrementAndGet();
            }, buffer.toString());
            System.out.println("parsed::" + totalCounter.get());
        }
    }


    final MongoHelper mongo = new MongoHelper();

    public void importLabelsToMongo() throws Exception {
        System.out.println("importing labels...");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)))) {
            final StringBuilder buffer = new StringBuilder();
            String line = null;
            int counter = 0;
            final AtomicLong totalCounter = new AtomicLong(0L);
            while ((line = reader.readLine()) != null) {
                if (line.charAt(0) == '#') continue;
                buffer.append(line).append("\n");
                if (++counter == 10_000L) {
                    parseLabel(triple -> {
                        totalCounter.incrementAndGet();
                        mongo.insertTripleLabel(triple);
                    }, buffer.toString());
                    System.out.format("%,8d", totalCounter.get());
                    System.out.print("\r");
                    counter = 0;
                    buffer.delete(0, buffer.length());
                }
            }
            parseLabel(triple -> {
                mongo.insertTripleLabel(triple);
                totalCounter.incrementAndGet();
            }, buffer.toString());
            System.out.println("parsed::" + totalCounter.get());
        }
    }

    public static void main(String[] args) throws Exception {
        boolean useMongo = false;
        for (String arg : args) {
            if (arg.equals("-m")) {
                useMongo = true;
            }
        }
        final TitleIndexer labelsImporter = new TitleIndexer(Environment.getInputFile("labels"));
        if (useMongo) {
            labelsImporter.importLabelsToMongo();
        } else
            labelsImporter.importLabelsToSolr();

    }

}
