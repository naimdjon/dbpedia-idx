package dbpedia_idx;

import no.westerdals.dbpedia_idx.TitleIndexer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ParseTest {

    @Test
    public void testParse() throws Exception {
        new TitleIndexer(null).parseLabel(triple -> {
            assertEquals("accessiblecomputing", triple.subject);
            assertEquals("label", triple.predicate);
            assertEquals("accessiblecomputing", triple.property);
        }, sample);
    }

    static final String sample =
            "<http://dbpedia.org/resource/AccessibleComputing> <http://www.w3.org/2000/01/rdf-schema#label> \"AccessibleComputing\"@en .";
}
