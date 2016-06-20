package org.ericski.mongoleader;

import com.ericski.mongoleader.MongoLeader;
import com.ericski.mongoleader.MongoLeaderBuilder;
import com.mongodb.MongoClient;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.*;
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
		MongoLeader instance = new MongoLeader(mc, LEADERKEY, "main");
		MongoLeader other = new MongoLeader(mc, LEADERKEY, "testExpiredLeader");
		instance.heartbeat();
		boolean result = other.amLeader();
		assertEquals(false, result);
		while (instance.memberCount() > 1)
		{
			TimeUnit.SECONDS.sleep(1);
			other.heartbeat();
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
