package no.westerdals.dbpedia_idx.index;

import com.google.common.base.Preconditions;
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
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.nio.file.Paths;

import static org.apache.lucene.index.IndexWriterConfig.OpenMode.CREATE_OR_APPEND;
import static org.apache.lucene.store.FSDirectory.open;

public class Indexer implements AutoCloseable{

    final IndexWriter writer;
    static boolean created = false;
    private final Searcher searcher;

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
        searcher=Searcher.create(indexPath);
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
        doc.add(new StringField("title", triple.subject.toLowerCase().replaceAll("[_()]"," "), Store.YES));
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
        final Term title_orig = new Term("title_orig", triple.subject);
        final Document document = searcher.findDocument(title_orig);
        document.add(new StringField("subject",triple.property.replaceAll("[_()]"," "),Store.YES));
        try {
            writer.updateDocument(title_orig,document);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
