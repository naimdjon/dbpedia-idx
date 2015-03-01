package no.westerdals.dbpedia_idx.index;

import com.google.common.base.Predicate;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.nio.file.Paths;

import static org.apache.lucene.store.FSDirectory.open;


public class Searcher implements AutoCloseable {

    private IndexSearcher searcher;
    private Analyzer analyzer;
    IndexReader reader;

    private Searcher(String indexPath) {
        try {
            this.reader = DirectoryReader.open(open(Paths.get(indexPath)));
            this.searcher = new IndexSearcher(reader);
            this.analyzer = new StandardAnalyzer();
        } catch (IOException e) {
            System.err.println("Could not create searcher, continuing without searching capabilities.");
        }
    }

    public static Searcher create(final String indexPath) {
        return new Searcher(indexPath);
    }

    public Document doc(final int i) throws IOException {
        return reader.document(i);
    }

    public void visitAllDocs(final Predicate<DocumentInformation> visitor) throws ParseException, IOException {
        for (int i = 0; i < reader.maxDoc(); i++) {
            visitor.apply(new DocumentInformation(reader.document(i).get("title_md5"), i));
        }
    }

    public TopDocs searchAll(final String queryString) throws ParseException {
        try {
            return searcher.search(new QueryParser("title", analyzer).parse(queryString), null, Integer.MAX_VALUE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }


    public Document search(final Term title_md5) {
        try {
            final TopDocs topDocs = searcher.search(new TermQuery(title_md5), 2);
            if (topDocs.totalHits > 1) {
                throw new RuntimeException("Duplicate ids:" + title_md5);
            } else if (topDocs.totalHits == 1) {
                return doc(topDocs.scoreDocs[0].doc);
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}