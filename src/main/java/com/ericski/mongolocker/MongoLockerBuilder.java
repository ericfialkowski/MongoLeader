package com.ericski.mongolocker;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.util.concurrent.TimeUnit;

public class MongoLockerBuilder
{
	private MongoClient mongoClient;
	private String lockKey;
	private String lockCollection = MongoLocker.DEFAULT_LOCK_COLLECTION;
	private long ttl = MongoLocker.DEFAULT_LOCK_LIFE;
	private String dbName;
	private MongoDatabase db = null;
	private String meta = null;

	public MongoLockerBuilder()
	{
	}

	public MongoLockerBuilder usingClient(MongoClient mongoClient)
	{
		this.mongoClient = mongoClient;
		return this;
	}

	public MongoLockerBuilder withKey(String leaderKey)
	{
		this.lockKey = leaderKey;
		return this;
	}


	public MongoLockerBuilder withLockCollection(String lockCollection)
	{
		this.lockCollection = lockCollection;
		return this;
	}

	public MongoLockerBuilder withTTL(long ttlSeconds)
	{
		this.ttl = ttlSeconds;
		return this;
	}

	public MongoLockerBuilder withTTL(long ttl, TimeUnit unit)
	{
		this.ttl = unit.toSeconds(ttl);
		return this;
	}

	public MongoLockerBuilder usingDB(String dbName)
	{
		this.dbName = dbName;
		return this;
	}

	public MongoLockerBuilder usingDB(MongoDatabase db)
	{
		this.db = db;
		return this;
	}

	public MongoLockerBuilder withMeta(String meta)
	{
		this.meta = meta;
		return this;
	}

	public MongoLocker build()
	{
		if(db != null)
			return new MongoLocker(lockKey,mongoClient, lockCollection, ttl, db, meta);
		else if(dbName != null)
			return new MongoLocker(lockKey,mongoClient, lockCollection, ttl, dbName, meta);

		throw new IllegalArgumentException("Must provide a mongodb object or database name");
	}
}
