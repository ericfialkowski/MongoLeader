package org.ericski.mongoleader;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

public class MongoLeader {

    private static final String HEARTBEAT_FIELD = "heartbeat";
    private static final String ID_FIELD = "_id";

    private final MongoDatabase database;
    private final MongoCollection<Document> leaders;
    private final ObjectId id;
    private final Document hb;
    private final Bson filter;
    private final UpdateOptions uo;

    public MongoLeader(MongoClient mongoClient, String leaderKey, String meta) {
        this(mongoClient, leaderKey, 5L, "leader_key", meta);
    }

    public MongoLeader(MongoClient mongoClient, String leaderKey, long ttl, String dbName, String meta) {
        database = mongoClient.getDatabase(dbName);
        leaders = database.getCollection(leaderKey);

        IndexOptions indexOptions = new IndexOptions();
        indexOptions.expireAfter(ttl, TimeUnit.SECONDS);

        leaders.createIndex(new Document(HEARTBEAT_FIELD, 1), indexOptions);
        id = new ObjectId();
        hb = new Document(HEARTBEAT_FIELD, -1).append(ID_FIELD, id).append("meta", meta);
        filter = Filters.eq(ID_FIELD, id);
        uo = new UpdateOptions();
        uo.upsert(true);
        System.out.println(id);
    }

    public synchronized void heartbeat() {
        hb.replace(HEARTBEAT_FIELD, new Date());
        leaders.updateOne(filter, new Document("$set", hb), uo);
    }

    public boolean amLeader() {
        heartbeat();
        Document leader = leaders.find().sort(Sorts.ascending(ID_FIELD)).limit(1).first();
        if (leader != null) {
            return id.equals(leader.getObjectId(ID_FIELD));
        }
        return false;
    }

    public long memberCount() {
        return leaders.count();
    }

    public void stepDown() {
        leaders.findOneAndDelete(filter);
    }
}
