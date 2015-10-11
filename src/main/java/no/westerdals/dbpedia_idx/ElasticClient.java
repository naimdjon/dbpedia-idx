package no.westerdals.dbpedia_idx;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticClient {
    private final Client client;
    private final String collection;
    private final ScheduledExecutorService scheduledExecutorService;

    public ElasticClient(String index) {
        this.collection = index;
        client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        Runtime.getRuntime().addShutdownHook(new Thread(client::close));
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                sendBulk();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 123L, 1000L, TimeUnit.MILLISECONDS);
    }

    private final Queue<Triple> indexingQueue = new ConcurrentLinkedQueue<>();

    public void insertTripleLabel(Triple triple) {
        indexingQueue.add(triple);
    }

    private void sendBulk() {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        while (!indexingQueue.isEmpty()) {
            int i = 0;
            while (i++ < 10000) {
                final Triple triple = indexingQueue.poll();
                if (triple == null) {
                    break;
                }
                addDocumentToBulk(bulkRequest, triple);
                if (i % 1000 == 0) {
                    sendBulkRequest(bulkRequest);
                    bulkRequest = client.prepareBulk();
                }
            }
            sendBulkRequest(bulkRequest);
        }
    }

    private void sendBulkRequest(BulkRequestBuilder bulkRequest) {
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
                            .setSource(jsonBuilder()
                                            .startObject()
                                            .field("subject", triple.subject)
                                            .field("subjectLower", triple.subjectLowerCased)
                                            .field("value", triple.property)
                                            .endObject()
                            ));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
