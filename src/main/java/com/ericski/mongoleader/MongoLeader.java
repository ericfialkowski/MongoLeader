package com.ericski.mongoleader;

import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.IndexOptions;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

public class MongoLeader implements AutoCloseable
{
	public static final String DEFAULT_LEADER_COLLECTION = "leader_keys";
	public static final long DEFAULT_LOCK_LIFE = 5;

	private static final String EXPIRES_FIELD = "expires";
	private static final String LOCK_FIELD = "lock_key";
	private static final String META_FIELD = "meta";
	private static final String ID_FIELD = "_id";

	private final AtomicBoolean steppedDown = new AtomicBoolean();

	private final MongoDatabase database;
	private final MongoCollection<Document> leaders;

	private final String lockName;
	private final long ttl;
	private final ObjectId id;

	private final Document lockDefinition;
	private final Bson filter;
	private final FindOneAndReplaceOptions updateOptions;

	MongoLeader(String lockName, MongoClient mongoClient, String leaderKey, long ttl, String dbName, String meta)
	{
		this(lockName, mongoClient, leaderKey, ttl, mongoClient.getDatabase(dbName), meta);
	}

	MongoLeader(String lockName, MongoClient mongoClient, String leaderKey, long ttl, MongoDatabase database, String meta)
	{
		this.lockName = lockName;
		this.ttl = ttl;

		this.database = database;
		leaders = this.database.getCollection(leaderKey);

		// ensure the ttl index on the collection
		IndexOptions indexOptions = new IndexOptions();
		indexOptions.expireAfter(0L, TimeUnit.SECONDS);
		indexOptions.name("lock_expiration_ttl_ndx");

		leaders.createIndex(new Document(EXPIRES_FIELD, 1), indexOptions);

		indexOptions = new IndexOptions();
		indexOptions.name("unique_lock_key_ndx");
		indexOptions.unique(true);
		leaders.createIndex(new Document(LOCK_FIELD, 1), indexOptions);

		id = new ObjectId();
		lockDefinition = new Document(LOCK_FIELD, this.lockName)
			.append(ID_FIELD, id)
			.append(EXPIRES_FIELD, expirationDate());

		if (meta != null && !meta.isEmpty())
		{
			lockDefinition.append(META_FIELD, meta);
		}
		filter = Filters.eq(ID_FIELD, id);

		updateOptions = new FindOneAndReplaceOptions();
		updateOptions.upsert(true);
	}

	boolean heartbeat()
	{
		boolean stillLeader;
		lockDefinition.replace(EXPIRES_FIELD, expirationDate());
		try
		{
			leaders.findOneAndReplace(filter, lockDefinition, updateOptions);
			stillLeader = true;
		}
		catch (MongoCommandException mce)
		{
			// dupe key, likely no longer the leader
			stillLeader = false;
		}
		return stillLeader;
	}

	private Date expirationDate()
	{
		LocalDateTime localDateTime = new Date().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
		localDateTime = localDateTime.plusSeconds(ttl);
		Date expire = Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
		return expire;
	}

	public boolean amLeader()
	{
		return heartbeat();
	}

	long memberCount()
	{
		return leaders.count();
	}

	public void stepDown()
	{
		if (steppedDown.compareAndSet(false, true))
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
