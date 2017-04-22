package com.ericski.mongolocker;

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

public class MongoLocker implements AutoCloseable
{
	public static final String DEFAULT_LOCK_COLLECTION = "locker_keys";
	public static final long DEFAULT_LOCK_LIFE = 5;

	private static final String EXPIRES_FIELD = "expires";
	private static final String LOCK_FIELD = "lock_key";
	private static final String META_FIELD = "meta";
	private static final String ID_FIELD = "_id";

	private final AtomicBoolean lockReleased = new AtomicBoolean();

	private final MongoDatabase database;
	private final MongoCollection<Document> locks;

	private final String lockName;
	private final long ttl;
	private final ObjectId id;

	private final Document lockDefinition;
	private final Bson filter;
	private final FindOneAndReplaceOptions updateOptions;

	MongoLocker(String lockName, MongoClient mongoClient, String lockCollectionName, long ttl, MongoDatabase database, String meta)
	{
		this.lockName = lockName;
		this.ttl = ttl;

		this.database = database;
		locks = this.database.getCollection(lockCollectionName);

		// ensure the ttl index on the collection
		IndexOptions indexOptions = new IndexOptions();
		indexOptions.expireAfter(0L, TimeUnit.SECONDS);
		indexOptions.name("lock_expiration_ttl_ndx");
		locks.createIndex(new Document(EXPIRES_FIELD, 1), indexOptions);

		indexOptions = new IndexOptions();
		indexOptions.name("unique_lock_key_ndx");
		indexOptions.unique(true);
		locks.createIndex(new Document(LOCK_FIELD, 1), indexOptions);

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

	private boolean checkLock()
	{
		boolean stillHeld;
		lockDefinition.replace(EXPIRES_FIELD, expirationDate());
		try
		{
			locks.findOneAndReplace(filter, lockDefinition, updateOptions);
			stillHeld = true;
		}
		catch (MongoCommandException mce)
		{
			if(mce.getCode() == 11000)
			{
				// dupe key, someone else holds the lock
				stillHeld = false;
			}
			else
				throw mce;
		}
		return stillHeld;
	}

	private Date expirationDate()
	{
		LocalDateTime expirationDateTime = LocalDateTime.now().plusSeconds(ttl);
		Date expirationDate = Date.from(expirationDateTime.atZone(ZoneId.systemDefault()).toInstant());
		return expirationDate;
	}

	public boolean haveLock()
	{
		return checkLock();
	}

	public void release()
	{
		if (lockReleased.compareAndSet(false, true))
		{
			locks.findOneAndDelete(filter);
		}
	}

	@Override
	public void close() throws Exception
	{
		release();
	}
}
