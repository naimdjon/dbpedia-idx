package no.westerdals.dbpedia_idx;

import com.google.common.base.Splitter;
import no.westerdals.dbpedia_idx.index.Indexer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBPediaShortAbstractIndexer {
    final Pattern shortAbstractPattern = Pattern.compile("\\s*?<http://dbpedia.org/resource/(.*?)>\\s+<http://www.w3.org/2000/01/rdf-schema#(.*?)>\\s+\"(.*?)\".*");
    private final String inputFile;
    private static long batchSize = 14_000L;

    public DBPediaShortAbstractIndexer(final String inputFile) {
        this.inputFile = inputFile;
    }

    public void parse(final String content, final TripleProcessor tripleProcessor) throws Exception {
        Splitter.on("@en .\n")
                .omitEmptyStrings()
                .split(content)
                .forEach(shortAbstractConsumer(tripleProcessor));
    }

    private Consumer<String> shortAbstractConsumer(final TripleProcessor tripleProcessor) {
        return line -> {
            final Matcher matcher = shortAbstractPattern.matcher(line);
            if (matcher.find()) {
                tripleProcessor.process(new Triple(matcher.group(1), matcher.group(2), matcher.group(3)));
            } else {
                System.err.println("No match for line " + line);
            }
        };
    }


    public void index() throws Exception {
        try (final Indexer indexer = Indexer.create(Environment.getIndexDir())) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)))) {
                final StringBuilder buffer = new StringBuilder();
                String line = null;
                int counter = 0;
                final AtomicLong totalCounter = new AtomicLong(0L);

                while ((line = reader.readLine()) != null) {
                    if (line.charAt(0) == '#') continue;
                    buffer.append(line).append("\n");
                    if (++counter == batchSize) {
                        parse(buffer.toString(), triple -> {
                            indexer.addToIndex(triple);
                            totalCounter.incrementAndGet();
                        });
                        System.out.format("%,8d", totalCounter.get());
                        System.out.print("\r");
                        counter = 0;
                        buffer.delete(0, buffer.length());
                    }
                }
                parse(buffer.toString(), triple -> {
                    indexer.addToIndex(triple);
                    totalCounter.incrementAndGet();
                });
                System.out.println("parsed::" + totalCounter.get());
            }
        }
        System.out.println("Done!");
    }

}
