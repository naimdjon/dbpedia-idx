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
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.nio.file.Paths;

import static org.apache.lucene.index.IndexWriterConfig.OpenMode.CREATE;
import static org.apache.lucene.store.FSDirectory.open;

public class Indexer implements AutoCloseable{

    final IndexWriter writer;
    static boolean created = false;

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
        iwc.setOpenMode(CREATE);
        writer = new IndexWriter(dir, iwc);
    }

    public void addToIndex(final Triple triple) {
        final Document doc = new Document();
        doc.add(new StringField("title", triple.subject, Store.YES));
        doc.add(new TextField(triple.predicate, triple.property, Store.YES));
        try {
            writer.addDocument(doc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
}
