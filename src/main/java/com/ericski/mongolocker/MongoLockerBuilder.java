package com.ericski.mongolocker;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.util.concurrent.TimeUnit;

public class MongoLockerBuilder
{
	private MongoClient mongoClient;
	private String lockKey;
	private String lockCollection = MongoLocker.DEFAULT_LOCK_COLLECTION;
	private long ttl = MongoLocker.DEFAULT_LOCK_SECONDS_TTL;
	private String dbName;
	private MongoDatabase db;
	private String meta;

	public MongoLockerBuilder()
	{
	}

	/**
	 *
	 * @param mongoClient
	 * @return the builder
	 */
	public MongoLockerBuilder usingClient(MongoClient mongoClient)
	{
		this.mongoClient = mongoClient;
		return this;
	}

	/**
	 *
	 * @param lockKey
	 * @return
	 */
	public MongoLockerBuilder withKey(String lockKey)
	{
		this.lockKey = lockKey;
		return this;
	}

	/**
	 *
	 * @param lockCollection
	 * @return
	 */
	public MongoLockerBuilder withLockCollection(String lockCollection)
	{
		this.lockCollection = lockCollection;
		return this;
	}

	/**
	 *
	 * @param ttlSeconds
	 * @return
	 */
	public MongoLockerBuilder withTTL(long ttlSeconds)
	{
		this.ttl = ttlSeconds;
		return this;
	}

	/**
	 *
	 * @param ttl
	 * @param unit
	 * @return
	 */
	public MongoLockerBuilder withTTL(long ttl, TimeUnit unit)
	{
		this.ttl = unit.toSeconds(ttl);
		return this;
	}

	/**
	 *
	 * @param dbName
	 * @return
	 */
	public MongoLockerBuilder usingDB(String dbName)
	{
		this.dbName = dbName;
		return this;
	}

	/**
	 *
	 * @param db
	 * @return
	 */
	public MongoLockerBuilder usingDB(MongoDatabase db)
	{
		this.db = db;
		return this;
	}

	/**
	 *
	 * @param meta
	 * @return
	 */
	public MongoLockerBuilder withMeta(String meta)
	{
		this.meta = meta;
		return this;
	}

	/**
	 * Creates a MongoLocker with the provided parameters
	 *
	 * @return instance of MongoLocker
	 * @throws IllegalArgumentException if not all required options are set
	 */
	public MongoLocker build()
	{
		if(mongoClient == null)
			throw new IllegalArgumentException("Must provide a MongoClient");

		if(lockKey == null || lockKey.isEmpty())
			throw new IllegalArgumentException("Null or empty lock key passed in");


		if (db != null)
			return new MongoLocker(lockKey, mongoClient, lockCollection, ttl, db, meta);
		else if (dbName != null)
			return new MongoLocker(lockKey, mongoClient, lockCollection, ttl, mongoClient.getDatabase(dbName), meta);

		throw new IllegalArgumentException("Must provide a mongodb object or database name");
	}
}
