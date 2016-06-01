package org.ericski.mongoleader;

import com.mongodb.MongoClient;

public class MongoLeaderBuilder
{
	private MongoClient mongoClient;
	private String leaderKey;
	private long ttl = MongoLeader.DEFAULT_LOCK_LIFE;
	private String dbName = MongoLeader.DEFAULT_LEADER_DB;
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
		this.leaderKey = leaderKey;
		return this;
	}

	public MongoLeaderBuilder withTTL(long ttl)
	{
		this.ttl = ttl;
		return this;
	}

	public MongoLeaderBuilder usingDB(String dbName)
	{
		this.dbName = dbName;
		return this;
	}

	public MongoLeaderBuilder withMeta(String meta)
	{
		this.meta = meta;
		return this;
	}

	public MongoLeader build()
	{
		return new MongoLeader(mongoClient, leaderKey, ttl, dbName, meta);
	}
}
