package org.ericski.mongoleader;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

@Ignore
public class MongoLeaderTest
{
	MongoClient mc;
	static String LEADERKEY = "leaders";

	public MongoLeaderTest()
	{
		MongoCredential etlGaugeCredential = MongoCredential.createScramSha1Credential("leader", "admin", "leader".toCharArray());
		List<MongoCredential> credentials = new ArrayList<>();
		credentials.add(etlGaugeCredential);
		mc = new MongoClient(new ServerAddress(), credentials);
	}


	@Test
	public void testLeader()
	{
		MongoLeader instance = new MongoLeader(mc, LEADERKEY,"main");
		boolean result = instance.amLeader();
		assertEquals(true, result);
		instance.stepDown();
	}

	@Test
	public void testNotLeader()
	{
		MongoLeader instance = new MongoLeader(mc, LEADERKEY,"main");
		MongoLeader other = new MongoLeader(mc, LEADERKEY,"testNotLeader");
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
		MongoLeader instance = new MongoLeader(mc, LEADERKEY,"main");
		MongoLeader other = new MongoLeader(mc, LEADERKEY,"testExpiredLeader");
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
