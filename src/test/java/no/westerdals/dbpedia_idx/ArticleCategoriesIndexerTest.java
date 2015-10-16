package no.westerdals.dbpedia_idx;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ArticleCategoriesIndexerTest {

    private ElasticClient mock;
    private ArticleCategoriesIndexer articleCategoriesIndexer;
    private TripleWithCategoryInserter tripleWithCategoryInserter;

    @Before
    public void setUp() {
        mock = mock(ElasticClient.class);
        articleCategoriesIndexer = new ArticleCategoriesIndexer(null, mock);
        tripleWithCategoryInserter = new TripleWithCategoryInserter(mock);
    }


    @Test
    public void processLine_simpleLine() throws Exception {
        processLineAndAssert(
                "Albedo"
                , "climate_forcing"
                , "<http://dbpedia.org/resource/Albedo> <http://purl.org/dc/terms/subject> <http://dbpedia.org/resource/Category:Climate_forcing> ."
        );
    }

    @Test
    public void processLine_twoCategories() throws Exception {
        processLineAndAssert(
                "Albedo"
                , "climate_forcing"
                , "<http://dbpedia.org/resource/Albedo> <http://purl.org/dc/terms/subject> <http://dbpedia.org/resource/Category:Climate_forcing> ."
                , "<http://dbpedia.org/resource/Albedo> <http://purl.org/dc/terms/subject> <http://dbpedia.org/resource/Category:Forcing_Client> ."
        );

        verify(mock).insertTripleLabel(argThat(new ArgumentMatcher<Triple>() {
            @Override
            public boolean matches(Object argument) {
                final Triple arg = (Triple) argument;
                assertThat(arg.getCategories()).containsAll(ImmutableSet.of(new Category("climate_forcing"), new Category("forcing_client")));
                return true;
            }
        }));
    }

    @Test
    public void createEntityWithCategories_insertNotCalled() throws Exception {
        final Triple result = tripleWithCategoryInserter.createEntityWithCategories(ImmutableSet.of(
                new Triple("1", "", "test")
                , new Triple("1", "", "test2")
                , new Triple("1", "", "test3")
        ));
        final Set<Category> categories = result.getCategories();
        assertThat(categories).hasSize(3);
        assertThat(categories).containsAll(ImmutableSet.of(
                new Category("test")
                , new Category("test2")
                , new Category("test3")
        ));
        verify(mock, times(0)).insertTripleLabel(any());
    }

    @Test(expected = IllegalArgumentException.class)
    public void createEntityWithCategories_throwsException() throws Exception {
        tripleWithCategoryInserter.createEntityWithCategories(ImmutableSet.of(
                new Triple("1", "", "test")
                , new Triple("2", "", "test2")
        ));
    }

    @Test
    public void currentBatchSize_nullProperty() throws Exception {
        tripleWithCategoryInserter.insertTripleWithCategories(new Triple("", null, null));
        assertThat(tripleWithCategoryInserter.getCurrentBatchSize()).isEqualTo(0);
    }

    @Test
    public void currentBatchSize_emptyProperty() throws Exception {
        tripleWithCategoryInserter.insertTripleWithCategories(new Triple("", null, ""));
        assertThat(tripleWithCategoryInserter.getCurrentBatchSize()).isEqualTo(0);
    }

    @Test
    public void currentBatchSize_blankProperty() throws Exception {
        tripleWithCategoryInserter.insertTripleWithCategories(new Triple("", null, " "));
        assertThat(tripleWithCategoryInserter.getCurrentBatchSize()).isEqualTo(0);
    }

    @Test
    public void currentBatchSize_nonBlankProperty() throws Exception {
        tripleWithCategoryInserter.insertTripleWithCategories(new Triple("", null, "test"));
        assertThat(tripleWithCategoryInserter.getCurrentBatchSize()).isEqualTo(1);
    }

    @Test
    public void insertTripleWithCategories_consequtiveLines() throws Exception {
        tripleWithCategoryInserter.insertTripleWithCategories(new Triple("1", "subject", "test"));
        tripleWithCategoryInserter.insertTripleWithCategories(new Triple("1", "subject", "test2"));
        tripleWithCategoryInserter.insertTripleWithCategories(new Triple("1", "subject", "test3"));
        tripleWithCategoryInserter.insertTripleWithCategories(new Triple("2", "subject", "test4"));
        tripleWithCategoryInserter.insertTripleWithCategories(new Triple("2", "subject", "test5"));
        verify(mock, times(1)).insertTripleLabel(new Triple("1", "subject", "test", ImmutableSet.of(
                new Category("test")
                , new Category("test2")
                , new Category("test3"))
        ));
        assertThat(tripleWithCategoryInserter.finishBatch()).isEqualTo(2);
    }


    void processLineAndAssert(final String wantedSubject, final String wantedValue, String... lines) throws Exception {
        for (String line : lines) {
            articleCategoriesIndexer.processLine(line);
        }
        final int finalSize = articleCategoriesIndexer.finishBatch();
        assertThat(finalSize).isEqualTo(lines.length);
        verify(mock, times(1)).insertTripleLabel(any());
        verify(mock).insertTripleLabel(argThat(new ArgumentMatcher<Triple>() {
            @Override
            public boolean matches(Object argument) {
                final Triple arg = (Triple) argument;
                assertThat(arg.subject).isEqualTo(wantedSubject);
                assertThat(arg.predicate).isEqualTo("subject");
                assertThat(arg.property).isEqualTo(wantedValue);
                return argument.equals(new Triple(wantedSubject, "subject", wantedValue));
            }
        }));
    }
}
