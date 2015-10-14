package dbpedia_idx;

import no.westerdals.dbpedia_idx.ElasticClient;
import no.westerdals.dbpedia_idx.LabelIndexer;
import no.westerdals.dbpedia_idx.Triple;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LabelIndexerTest {

    private ElasticClient mock;

    @Before
    public void setUp() {
        mock = mock(ElasticClient.class);
    }

    @Test
    public void counterIncremented() throws Exception {
        final int counter = new LabelIndexer(null, mock).processLine("test", 1);
        assertThat(counter).isEqualTo(2);
    }

    @Test
    public void commentsNotProcessed() {
        final int counter = new LabelIndexer(null, mock).processLine("#tets", 0);
        assertThat(counter).isEqualTo(0);
        verify(mock, times(0)).insertTripleLabel(any());
    }

    @Test
    public void simple() throws Exception {
        processLineAndAssert("<http://dbpedia.org/resource/AccessibleComputing> <http://www.w3.org/2000/01/rdf-schema#label> \"AccessibleComputing\"@en ."
                , "AccessibleComputing"
                , "accessiblecomputing"
        );
    }

    @Test
    public void subjectContainsSlash() throws Exception {
        processLineAndAssert("<http://dbpedia.org/resource/Andorra/Transnational_issues> <http://www.w3.org/2000/01/rdf-schema#label> \"Andorra/Transnational issues\"@en ."
                , "Andorra/Transnational_issues"
                , "andorra/transnational issues");
    }

    public void processLineAndAssert(String line, final String wantedSubject, final String wantedValue) throws Exception {
        new LabelIndexer(null, mock).processLine(line, 1);
        verify(mock, times(1)).insertTripleLabel(any());
        verify(mock).insertTripleLabel(argThat(new ArgumentMatcher<Triple>() {
            @Override
            public boolean matches(Object argument) {
                return argument.equals(new Triple(wantedSubject, "label", wantedValue));
            }
        }));
    }
}
