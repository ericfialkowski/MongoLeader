package com.ericski.mongolocker;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class MongoLockerTest
{

	private final MongoClient mc;
	private static String LEADERKEY = "locks";
	private static final String TEST_DATABASE_NAME = "lock_tester";

	public MongoLockerTest()
	{
		mc = new MongoClient();
	}

	@Before
	public void cleanUp()
	{
		mc.dropDatabase(TEST_DATABASE_NAME);
	}

	@Test
	public void testBasicLock()
	{
		System.out.println("testBasicLock");
		MongoLocker instance = new MongoLockerBuilder().usingDB(TEST_DATABASE_NAME).usingClient(mc).withKey(LEADERKEY).withMeta("main testBasicLock").build();
		boolean result = instance.haveLock();
		assertEquals(true, result);
		instance.release();
	}

	@Test
	public void testNotLockHolder()
	{
		System.out.println("testNotLockHolder");
		MongoLocker instance = new MongoLockerBuilder().usingDB(TEST_DATABASE_NAME).usingClient(mc).withKey(LEADERKEY).withMeta("main testNotLockHolder").build();
		MongoLocker other = new MongoLockerBuilder().usingDB(TEST_DATABASE_NAME).usingClient(mc).withKey(LEADERKEY).withMeta("other testNotLockHolder").build();
		instance.haveLock();
		boolean result = other.haveLock();
		assertEquals(false, result);
		result = instance.haveLock();
		assertEquals(true, result);
		other.release();
		instance.release();
	}

	@Test
	public void testExpiredLock() throws InterruptedException
	{
		System.out.println("testExpiredLock");
		MongoCollection<Document> leaderCollection = mc.getDatabase(TEST_DATABASE_NAME).getCollection(MongoLocker.DEFAULT_LOCK_COLLECTION);
		long failTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
		MongoLocker instance = new MongoLockerBuilder()
			.usingClient(mc)
			.usingDB(TEST_DATABASE_NAME)
			.withKey(LEADERKEY)
			.withMeta("main testExpiredLock")
			.withTTL(15)
			.build();
		MongoLocker other = new MongoLockerBuilder()
			.usingClient(mc)
			.usingDB(TEST_DATABASE_NAME)
			.withKey(LEADERKEY)
			.withMeta("other testExpiredLock")
			.withTTL(15)
			.build();

		instance.haveLock();
		boolean result = other.haveLock();
		assertEquals(false, result);
		while (leaderCollection.count() > 0)
		{
			TimeUnit.SECONDS.sleep(15);
			if (System.currentTimeMillis() > failTime)
			{
				fail("Didn't release lock in time");
			}
			System.out.println("Waiting for lock expiration");
		}
		result = other.haveLock();
		assertEquals(true, result);
		TimeUnit.SECONDS.sleep(5);
		result = instance.haveLock();
		assertEquals(false, result);
		other.release();
		instance.release();
	}
}
