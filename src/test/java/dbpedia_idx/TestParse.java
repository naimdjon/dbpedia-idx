package dbpedia_idx;

import no.westerdals.dbpedia_idx.NTParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestParse {

    @Test
    public void testParse() throws Exception {
        new NTParser().parseLabel(triple -> {
            assertEquals("AccessibleComputing", triple.subject);
            assertEquals("label", triple.predicate);
            assertEquals("AccessibleComputing", triple.object);
        }, sample);
    }

    static final String sample =
            "<http://dbpedia.org/resource/AccessibleComputing> <http://www.w3.org/2000/01/rdf-schema#label> \"AccessibleComputing\"@en .";
}
