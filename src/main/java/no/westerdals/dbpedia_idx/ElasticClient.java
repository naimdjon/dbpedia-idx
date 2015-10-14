package no.westerdals.dbpedia_idx;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticClient {
    private final Client client;
    private final String collection;
    private final ScheduledExecutorService scheduledExecutorService;

    private final int batchSize = 10000;
    private final int bulkSize = 1000;

    private final Queue<Triple> indexingQueue = new ConcurrentLinkedQueue<>();

    public static ElasticClient createDefaultClientForIndex(String index) {
        return new ElasticClient(
                index
                , newSingleThreadScheduledExecutor()
                , new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300))
        );
    }

    public ElasticClient(String index, ScheduledExecutorService executorService, Client client) {
        this.collection = index;
        this.client = client;
        Runtime.getRuntime().addShutdownHook(new Thread(client::close));
        this.scheduledExecutorService = executorService;
        this.scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                sendBulk();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 123L, 1000L, TimeUnit.MILLISECONDS);
    }

    public void insertTripleLabel(Triple entity) {
        indexingQueue.add(entity);
    }

    private void sendBulk() {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        while (!indexingQueue.isEmpty()) {
            int i = 0;
            while (i++ < batchSize) {
                final Triple Triple = indexingQueue.poll();
                if (Triple == null) {
                    break;
                }
                addDocumentToBulk(bulkRequest, Triple);
                if (i % bulkSize == 0) {
                    sendBulkRequest(bulkRequest);
                    bulkRequest = client.prepareBulk();
                }
            }
            sendBulkRequest(bulkRequest);
        }
    }

    private void sendBulkRequest(final BulkRequestBuilder bulkRequest) {
        if (bulkRequest.numberOfActions() <= 0) {
            return;
        }
        final BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            System.err.println("FAILED to bulk update the sendBulk.");
        }
    }

    private void addDocumentToBulk(BulkRequestBuilder bulkRequest, Triple triple) {
        try {
            bulkRequest.add(
                    client.prepareIndex("dbpedia", collection, triple.getMD5Title())
                            .setSource(createIndexEntry(triple)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public XContentBuilder createIndexEntry(Triple triple) throws IOException {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("entityId", triple.subject)
                .field("subjectLower", triple.subjectLowerCased);
        final Set<Category> categories = triple.getCategories();
        if (categories.isEmpty()) {
            builder = builder.field("value", triple.property);
        } else {
            builder = builder.array("value", categories.toArray(new Category[categories.size()]));
        }
        return builder.endObject();
    }

    public void shutdown() {
        while (!indexingQueue.isEmpty()) {
            System.out.println("indexingQueue:" + indexingQueue.size());
            try {
                Thread.sleep(5L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        scheduledExecutorService.shutdown();
    }

}
