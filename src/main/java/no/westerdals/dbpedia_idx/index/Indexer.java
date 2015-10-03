package no.westerdals.dbpedia_idx.index;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import no.westerdals.dbpedia_idx.Environment;
import no.westerdals.dbpedia_idx.MD5;
import no.westerdals.dbpedia_idx.Triple;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.lucene.index.IndexWriterConfig.OpenMode.CREATE_OR_APPEND;
import static org.apache.lucene.store.FSDirectory.open;

public class Indexer implements AutoCloseable {

    final IndexWriter writer;
    static boolean created = false;
    private final Searcher searcher;
    public static final Set<String> cache = Sets.newHashSet();

    public static void main(String[] args) throws IOException, ParseException {
        load(Environment.getIndexDir());
    }

    static {
        try {
            load(Environment.getIndexDir());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void load(final String indexPath) throws ParseException, IOException {
        final Searcher searcher = Searcher.create(indexPath);
        Stopwatch stopwatch = Stopwatch.createStarted();
        final TopDocs topDocs = searcher.searchAll("*:*");
        System.out.println("total hits:" + topDocs.totalHits);
        final AtomicInteger c = new AtomicInteger(0);
        searcher.visitAllDocs(input -> {
            cache.add(input.md5id);
            if (c.incrementAndGet() % 10_000L == 0) {
                System.out.format("%,8d", c.get());
                System.out.print("\r");
            }
            return true;
        });
        System.out.println("\n");
        System.out.println("Size:" + cache.size());
        System.out.println("took:" + stopwatch.elapsed(TimeUnit.SECONDS));
    }

    public static Indexer create(final String indexPath) {
        Preconditions.checkState(!created);
        try {
            created = true;
            return new Indexer(indexPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Indexer(final String indexPath) throws IOException {
        final Directory dir = open(Paths.get(indexPath));
        final Analyzer analyzer = new StandardAnalyzer();
        final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(CREATE_OR_APPEND);
        writer = new IndexWriter(dir, iwc);
        searcher = Searcher.create(indexPath);
    }

    public void addToIndex(final Triple triple) {
        try {
            writer.addDocument(createDocument(triple));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Document createDocument(Triple triple) {
        final Document doc = new Document();
        doc.add(new StringField("title_orig", triple.subject, Store.YES));
        doc.add(new StringField("title", triple.subject.toLowerCase().replaceAll("[_()]", " "), Store.YES));
        doc.add(new StringField("title_md5", triple.getMD5Title(), Store.YES));
        doc.add(new TextField(triple.predicate, triple.property.replaceAll("[_()]", " "), Store.YES));
        if ("03bf1e6c68492a0925f89b8749dd7e53".equals(triple.getMD5Title())) {
            System.out.println("\n ADDING NOW:"+doc);
        }
        return doc;
    }

    @Override
    public void close() {
        try {
            writer.commit();
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String,Document> currentlyAddingDocs = Maps.newHashMap();

    public void updateIndex(Triple triple) {
        try {
            final boolean existsInIndex = cache.contains(triple.getMD5Title());
            Document document;
            final Term title_md5 = new Term("title_md5", triple.getMD5Title());
            if (existsInIndex) {
                document = searcher.search(title_md5);
                if (document == null) {
                    document=currentlyAddingDocs.get(triple.getMD5Title());
                }
                Preconditions.checkNotNull(document,title_md5.toString());
                writer.deleteDocuments(title_md5);
                document.add(new StringField(triple.predicate, triple.property.replaceAll("[_()]", " "), Store.YES));
                writer.addDocument(document);
            } else {
                addToIndex(triple);
                writer.commit();
                document = searcher.search(title_md5);
                if (document == null) {
                    currentlyAddingDocs.put(triple.getMD5Title(),document);
                }
                cache.add(MD5.hash(triple.subject));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
