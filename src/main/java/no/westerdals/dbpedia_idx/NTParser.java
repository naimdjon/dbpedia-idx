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

public class NTParser {
    //final Pattern labelPattern1=Pattern.compile("\\s*?<(.*?)>\\s+<(.*?)>\\s+\"(.*?)\".*");
    final Pattern labelPattern = Pattern.compile("\\s*?<http://dbpedia.org/resource/(.*?)>\\s+<http://www.w3.org/2000/01/rdf-schema#(.*?)>\\s+\"(.*?)\".*");

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
                tripleProcessor.process(new Triple(matcher.group(1), matcher.group(2), matcher.group(3)));
            } else {
                System.err.println("No match for line " + line);
            }
        };
    }

    public static void main(String[] args) throws Exception {
        new NTParser().importLabels();
    }

    MongoHelper mongo = new MongoHelper();

    public void importLabels() throws Exception {
	    System.out.println("importing labels...");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/takhirov/Downloads/labels_en.nt")))) {
            final StringBuilder buffer = new StringBuilder();
            String line = null;
            int counter = 0;
            final AtomicLong totalCounter = new AtomicLong(0L);
            while ((line = reader.readLine()) != null) {
                if (line.charAt(0) == '#') continue;
                buffer.append(line).append("\n");
                if (++counter == 10000) {
                    parseLabel(triple -> {
                        totalCounter.incrementAndGet();
                        mongo.insertTripleLabel(triple);
                    }, buffer.toString());
                    System.out.print(totalCounter.get() + "\r");
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

}
