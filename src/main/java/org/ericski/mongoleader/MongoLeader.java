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
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

public class MongoLeader implements AutoCloseable
{
	public static final String DEFAULT_LEADER_DB = "leader_keys";
	public static final long DEFAULT_LOCK_LIFE = 5L;

	private static final String HEARTBEAT_FIELD = "heartbeat";
	private static final String META_FIELD = "meta";
	private static final String ID_FIELD = "_id";

	private final AtomicBoolean steppedDown = new AtomicBoolean();

	private final MongoDatabase database;
	private final MongoCollection<Document> leaders;
	private final ObjectId id;
	private final Document heartbeatDoc;
	private final Document setDoc;
	private final Bson filter;
	private final UpdateOptions uo;

	public MongoLeader(MongoClient mongoClient, String leaderKey)
	{
		this(mongoClient, leaderKey,null);
	}

	public MongoLeader(MongoClient mongoClient, String leaderKey, String meta)
	{
		this(mongoClient, leaderKey, DEFAULT_LOCK_LIFE, DEFAULT_LEADER_DB, meta);
	}

	public MongoLeader(MongoClient mongoClient, String leaderKey, long ttl, String dbName, String meta)
	{
		database = mongoClient.getDatabase(dbName);
		leaders = database.getCollection(leaderKey);

		IndexOptions indexOptions = new IndexOptions();
		indexOptions.expireAfter(ttl, TimeUnit.SECONDS);

		leaders.createIndex(new Document(HEARTBEAT_FIELD, 1), indexOptions);
		id = new ObjectId();
		heartbeatDoc = new Document(HEARTBEAT_FIELD, -1).append(ID_FIELD, id);
		if (meta != null && !meta.isEmpty())
		{
			heartbeatDoc.append(META_FIELD, meta);
		}
		setDoc = new Document("$set", heartbeatDoc);
		filter = Filters.eq(ID_FIELD, id);
		uo = new UpdateOptions();
		uo.upsert(true);
	}

	public synchronized void heartbeat()
	{
		heartbeatDoc.replace(HEARTBEAT_FIELD, new Date());
		leaders.updateOne(filter, setDoc, uo);
	}

	public boolean amLeader()
	{
		heartbeat();
		Document leader = leaders.find().sort(Sorts.ascending(ID_FIELD)).limit(1).first();
		if (leader != null)
		{
			return id.equals(leader.getObjectId(ID_FIELD));
		}
		return false;
	}

	public long memberCount()
	{
		return leaders.count();
	}

	public void stepDown()
	{
		if(steppedDown.compareAndSet(false, true))
		{
			leaders.findOneAndDelete(filter);
		}
	}

	@Override
	public void close() throws Exception
	{
		stepDown();
	}


}
