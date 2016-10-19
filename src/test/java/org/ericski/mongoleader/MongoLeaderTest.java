package org.ericski.mongoleader;

import com.ericski.mongoleader.MongoLeader;
import com.ericski.mongoleader.MongoLeaderBuilder;
import com.mongodb.MongoClient;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
//import org.junit.Ignore;
//
//@Ignore

public class MongoLeaderTest
{

	MongoClient mc;
	static String LEADERKEY = "leaders";

	public MongoLeaderTest()
	{
		mc = new MongoClient();
	}

	@Before
	public void cleanUp()
	{
		mc.dropDatabase(MongoLeader.DEFAULT_LEADER_DB);
	}

	@Test
	public void testLeader()
	{
		MongoLeader instance = new MongoLeaderBuilder().usingClient(mc).withKey(LEADERKEY).withMeta("main").build();
		boolean result = instance.amLeader();
		assertEquals(true, result);
		instance.stepDown();
	}

	@Test
	public void testNotLeader()
	{
		MongoLeader instance = new MongoLeaderBuilder().usingClient(mc).withKey(LEADERKEY).withMeta("main").build();
		MongoLeader other  = new MongoLeaderBuilder().usingClient(mc).withKey(LEADERKEY).withMeta("testNotLeader").build();
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
		long failTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
		MongoLeader instance = new MongoLeaderBuilder()
			.usingClient(mc)
			.withKey(LEADERKEY)
			.withMeta("main")
			.withTTL(5)
			.build();
		MongoLeader other = new MongoLeaderBuilder()
			.usingClient(mc)
			.withKey(LEADERKEY)
			.withMeta("testExpiredLeader")
			.withTTL(5,TimeUnit.SECONDS)
			.build();

		instance.heartbeat();
		boolean result = other.amLeader();
		assertEquals(false, result);
		while (instance.memberCount() > 1)
		{
			TimeUnit.SECONDS.sleep(2);
			other.heartbeat();
			if(System.currentTimeMillis() > failTime )
				fail("Didn't step down in time");
		}
		result = other.amLeader();
		assertEquals(true, result);
		TimeUnit.SECONDS.sleep(5);
		result = instance.amLeader();
		assertEquals(true, result);
		other.stepDown();
		instance.stepDown();
	}
}
