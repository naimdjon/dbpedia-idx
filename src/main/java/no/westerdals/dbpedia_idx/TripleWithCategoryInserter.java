package no.westerdals.dbpedia_idx;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import org.elasticsearch.common.lang3.StringUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

public class TripleWithCategoryInserter {

    private final LinkedList<Triple> batch = new LinkedList<>();
    private ElasticClient elasticClient;

    public TripleWithCategoryInserter(ElasticClient elasticClient) {
        this.elasticClient = elasticClient;
    }

    public int getCurrentBatchSize() {
        return batch.size();
    }

    public void insertTripleWithCategories(Triple triple) {
        final Triple peek = batch.peek();
        if (peek != null && !peek.subject.equals(triple.subject)) {
            sendToElasticAndClearBatch();
        }
        if (StringUtils.isNotBlank(triple.property)) {
            batch.add(triple);
        }
    }

    private void sendToElasticAndClearBatch() {
        elasticClient.insertTripleLabel(createEntityWithCategories(batch));
        batch.clear();
    }

    public Triple createEntityWithCategories(final Collection<Triple> triples) {
        final Triple result = triples.iterator().next();
        result.setCategories(new HashSet<>(Collections2.transform(triples, input -> {
            Preconditions.checkArgument(input.subject.equals(result.subject));
            return new Category(input.property);
        })));
        return result;
    }

    public int finishBatch() {
        final int finalSize = batch.size();
        if (finalSize > 0) {
            sendToElasticAndClearBatch();
        }
        return finalSize;
    }
}
