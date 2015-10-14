package no.westerdals.dbpedia_idx;

import com.google.common.collect.ImmutableSet;
import org.assertj.core.api.Assertions;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.mock;

public class ElasticClientTest {

    private ElasticClient elasticClient;

    @Before
    public void setUp() {
        elasticClient = new ElasticClient("testindex", mock(ScheduledExecutorService.class), mock(Client.class));
    }

    @Test
    public void createTripleObject_noCategory() throws Exception {
        final String content = elasticClient.createIndexEntry(new Triple("test", null, "testvalue")).string();
        Assertions.assertThat(content).isEqualTo("{\"entityId\":\"test\",\"subjectLower\":\"test\",\"value\":\"testvalue\"}");
    }

    @Test
    public void createTripleObject_withCategories() throws Exception {
        final Triple triple = new Triple("test", null, "testvalue");
        triple.setCategories(ImmutableSet.of(new Category("testcat1"),new Category("testcat2")));
        final String content = elasticClient.createIndexEntry(triple).string();
        Assertions.assertThat(content).isEqualTo("{\"entityId\":\"test\",\"subjectLower\":\"test\",\"value\":[\"testcat1\",\"testcat2\"]}");
    }
}