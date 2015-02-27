package no.westerdals.dbpedia_idx.index;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static org.apache.lucene.index.IndexWriterConfig.OpenMode.CREATE_OR_APPEND;
import static org.apache.lucene.store.FSDirectory.open;

public class Indexer implements AutoCloseable {

    final IndexWriter writer;
    static boolean created = false;
    private final Searcher searcher;

    public static void main(String[] args) throws ParseException, IOException {
        final Searcher s = Searcher.create("/Users/takhirov/NEEL_LUCENE_INDEX");
        Stopwatch stopwatch=Stopwatch.createStarted();
        final TopDocs topDocs = s.searchAll("*:*");
        System.out.println("total hits:"+topDocs.totalHits);
        final HashSet<Object> titles = Sets.newHashSet();
        int c=0;
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = topDocs.scoreDocs[i];
            final Document doc = s.doc(scoreDoc.doc);
            if (c++ % 100_000L == 0) {
                System.out.print("Counted: " + c);
                System.out.print("\r");
            }
            titles.add(doc.get("title_md5"));
        }
        System.out.println("\n");
        System.out.println("Size:"+titles.size());
        System.out.println("took:"+stopwatch.elapsed(TimeUnit.SECONDS));
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
        doc.add(new StringField("title_md5", MD5.hash(triple.subject), Store.YES));
        doc.add(new TextField(triple.predicate, triple.property.toLowerCase(), Store.YES));
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

    public void updateIndex(Triple triple) {
        final Term title_orig = new Term("title_md5", MD5.hash(triple.subject));
        final Document document = searcher.findDocument(title_orig);
        if (document == null) {
            addToIndex(triple);
        } else {
            document.add(new StringField(triple.predicate, triple.property.replaceAll("[_()]", " "), Store.YES));
            try {
                writer.updateDocument(title_orig, document);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    }
}
