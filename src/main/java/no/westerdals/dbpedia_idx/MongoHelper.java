package no.westerdals.dbpedia_idx;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;

public class MongoHelper {

    private final DBCollection labels;

    public MongoHelper() {
        try {
            final MongoClient mongoClient = new MongoClient();
            final DB db = mongoClient.getDB("dbpedia");
            this.labels = db.getCollection("labels");

        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertTripleLabel(final Triple Triple) {
        labels.insert(new BasicDBObject("s", Triple.subject).append("o", Triple.property));
    }
}
