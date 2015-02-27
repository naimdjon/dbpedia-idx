package no.westerdals.dbpedia_idx.index;

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

    private  IndexSearcher searcher;
    private Analyzer analyzer;
    private IndexReader reader;

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

    public TopDocs search(final String queryString) throws ParseException {
        try {
            return searcher.search(new QueryParser("title", analyzer).parse(queryString), null, 100);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public TopDocs searchAll(final String queryString) throws ParseException {
        try {
            return searcher.search(new QueryParser("title", analyzer).parse(queryString), null,Integer.MAX_VALUE );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }

    public Document findDocument(final Term term) {
        try {
            final TopDocs docs = searcher.search(new TermQuery(term), 1);
            if (docs.scoreDocs.length == 0) {
                return null;
            }
            return doc(docs.scoreDocs[0].doc);
        } catch (IOException e) {
            throw new RuntimeException(term+" not found",e);
        }
    }
}