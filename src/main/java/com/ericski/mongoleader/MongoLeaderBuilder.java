package com.ericski.mongoleader;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.util.concurrent.TimeUnit;

public class MongoLeaderBuilder
{
	private MongoClient mongoClient;
	private String lockKey;
	private String lockCollection = MongoLeader.DEFAULT_LEADER_COLLECTION;
	private long ttl = MongoLeader.DEFAULT_LOCK_LIFE;
	private String dbName;
	private MongoDatabase db = null;
	private String meta = null;

	public MongoLeaderBuilder()
	{
	}

	public MongoLeaderBuilder usingClient(MongoClient mongoClient)
	{
		this.mongoClient = mongoClient;
		return this;
	}

	public MongoLeaderBuilder withKey(String leaderKey)
	{
		this.lockKey = leaderKey;
		return this;
	}


	public MongoLeaderBuilder withLockCollection(String lockCollection)
	{
		this.lockCollection = lockCollection;
		return this;
	}

	public MongoLeaderBuilder withTTL(long ttlSeconds)
	{
		this.ttl = ttlSeconds;
		return this;
	}

	public MongoLeaderBuilder withTTL(long ttl, TimeUnit unit)
	{
		this.ttl = unit.toSeconds(ttl);
		return this;
	}

	public MongoLeaderBuilder usingDB(String dbName)
	{
		this.dbName = dbName;
		return this;
	}

	public MongoLeaderBuilder usingDB(MongoDatabase db)
	{
		this.db = db;
		return this;
	}

	public MongoLeaderBuilder withMeta(String meta)
	{
		this.meta = meta;
		return this;
	}

	public MongoLeader build()
	{
		if(db != null)
			return new MongoLeader(lockKey,mongoClient, lockCollection, ttl, db, meta);
		else if(dbName != null)
			return new MongoLeader(lockKey,mongoClient, lockCollection, ttl, dbName, meta);

		throw new IllegalArgumentException("Must provide a mongodb object or database name");
	}
}
