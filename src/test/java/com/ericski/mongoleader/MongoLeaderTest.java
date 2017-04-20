package com.ericski.mongoleader;

import com.mongodb.MongoClient;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class MongoLeaderTest
{

	private final MongoClient mc;
	private static String LEADERKEY = "leaders";
	private static final String TEST_DATABASE_NAME = "lock_tester";

	public MongoLeaderTest()
	{
		mc = new MongoClient();
	}

	@Before
	public void cleanUp()
	{
		mc.dropDatabase(TEST_DATABASE_NAME);
	}

	@Test
	public void testBasicLeader()
	{
		System.out.println("testBasicLeader");
		MongoLeader instance = new MongoLeaderBuilder().usingDB(TEST_DATABASE_NAME).usingClient(mc).withKey(LEADERKEY).withMeta("main testLeader").build();
		boolean result = instance.amLeader();
		assertEquals(true, result);
		instance.stepDown();
	}

	@Test
	public void testNotLeader()
	{
		System.out.println("testNotLeader");
		MongoLeader instance = new MongoLeaderBuilder().usingDB(TEST_DATABASE_NAME).usingClient(mc).withKey(LEADERKEY).withMeta("main testNotLeader").build();
		MongoLeader other = new MongoLeaderBuilder().usingDB(TEST_DATABASE_NAME).usingClient(mc).withKey(LEADERKEY).withMeta("other testNotLeader").build();
		instance.heartbeat();
		boolean result = other.amLeader();
		assertEquals(false, result);
		result = instance.amLeader();
		assertEquals(true, result);
		other.stepDown();
		instance.stepDown();
	}

	@Test
	public void testExpiredLeader() throws InterruptedException
	{
		System.out.println("testExpiredLeader");
		long failTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
		MongoLeader instance = new MongoLeaderBuilder()
			.usingClient(mc)
			.usingDB(TEST_DATABASE_NAME)
			.withKey(LEADERKEY)
			.withMeta("main testExpiredLeader")
			.withTTL(15)
			.build();
		MongoLeader other = new MongoLeaderBuilder()
			.usingClient(mc)
			.usingDB(TEST_DATABASE_NAME)
			.withKey(LEADERKEY)
			.withMeta("other testExpiredLeader")
			.withTTL(15)
			.build();

		instance.heartbeat();
		boolean result = other.amLeader();
		assertEquals(false, result);
		while (instance.memberCount() > 0)
		{
			TimeUnit.SECONDS.sleep(15);
			if (System.currentTimeMillis() > failTime)
			{
				fail("Didn't step down in time");
			}
			System.out.println("Waiting for lock expiration");
		}
		result = other.amLeader();
		assertEquals(true, result);
		TimeUnit.SECONDS.sleep(5);
		result = instance.amLeader();
		assertEquals(false, result);
		other.stepDown();
		instance.stepDown();
	}
}
