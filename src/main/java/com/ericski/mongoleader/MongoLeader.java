package com.ericski.mongoleader;

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
	public static final int DEFAULT_LOCK_LIFE = 5;

	private static final String EXPIRES_FIELD = "expires";
	private static final String HEARTBEAT_FIELD = "heartbeat";
	private static final String LOCK_FIELD = "lock_key";
	private static final String META_FIELD = "meta";
	private static final String ID_FIELD = "_id";

	private final AtomicBoolean steppedDown = new AtomicBoolean();

	private final MongoDatabase database;
	private final MongoCollection<Document> leaders;

	private final String lockName;
	private final int ttl;
	private final ObjectId id;


	private final Document lockDefinition;
	private final Document setDoc;
	private final Bson filter;
	private final UpdateOptions uo;

	MongoLeader(String lockName, MongoClient mongoClient, String leaderKey, int ttl, String dbName, String meta)
	{
		this(lockName,mongoClient, leaderKey, ttl, mongoClient.getDatabase(dbName), meta);
	}

	MongoLeader(String lockName, MongoClient mongoClient, String leaderKey, int ttl, MongoDatabase database, String meta)
	{
		this.lockName = lockName;
		this.ttl = ttl;

		this.database = database;
		leaders = this.database.getCollection(leaderKey);

		// ensure the ttl index on the collection
		IndexOptions indexOptions = new IndexOptions();
		indexOptions.expireAfter(0L, TimeUnit.SECONDS);
		leaders.createIndex(new Document(EXPIRES_FIELD, 1), indexOptions);

		id = new ObjectId();
		lockDefinition = new Document(LOCK_FIELD, this.lockName).append(ID_FIELD, id);
		if (meta != null && !meta.isEmpty())
		{
			lockDefinition.append(META_FIELD, meta);
		}
		setDoc = new Document("$set", lockDefinition);
		filter = Filters.eq(ID_FIELD, id);
		uo = new UpdateOptions();
		uo.upsert(true);
	}

	public synchronized void heartbeat()
	{
		lockDefinition.replace(HEARTBEAT_FIELD, new Date());
		leaders.updateOne(filter, setDoc, uo);
	}

	public boolean amLeader()
	{
		boolean iAm;
		Document leader = leaders.find().sort(Sorts.ascending(ID_FIELD)).limit(1).first();
		if (leader != null)
		{
			iAm = id.equals(leader.getObjectId(ID_FIELD));
			if(iAm)
				heartbeat();
		}
		else
		{
			heartbeat();
			iAm = true;
		}
		return iAm;
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
