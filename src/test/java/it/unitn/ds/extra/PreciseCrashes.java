package it.unitn.ds.extra;

import akka.testkit.javadsl.TestKit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import akka.actor.Actor;
import akka.actor.ActorRef;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.Client;
import it.unitn.ds.Messages;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;
import it.unitn.ds.AbstractReplica.CoordinatorElected;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.ElectionStarted;
import it.unitn.ds.AbstractReplica.UpdateApplied;


public class PreciseCrashes {
     
    @ParameterizedTest(name = "crash on Replica after N-th Update message => coordinator {0}, nodes {1}, threshold {2}")
    @CsvSource({
            "1,7,2",
            "0,22,3",
    })
    void crashAfterNthUpdateMessage(int coordinator, int n_nodes, int threshold) throws InterruptedException {
        final int TARGET = (coordinator + 1) % n_nodes;

        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "crashAfterNthUpdateMessage_" + coordinator + "_" + n_nodes + "_" + threshold, n_nodes, coordinator);
        TestKit clientProbe = new TestKit(sys.system);
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.ofNullable(sys.actors.get(coordinator)), clientProbe.getRef()),
                "client");

        sys.actors.get(TARGET).tell(new Crash(Crash.Type.Update, threshold), Actor.noSender());

        // The first [threshold - 1] writes are going through the update protocol succesfully
        for (int i = 0; i < threshold - 1; i++) {
            int value = 100 + i;
            client.tell(new AbstractClient.WriteRequest(i, value), Actor.noSender());
            WriteResult wr = (WriteResult) clientProbe.fishForMessage(
                    Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), "WriteResult",
                    msg -> msg instanceof WriteResult);
            assertEquals(new WriteResult(true, i, value, coordinator), wr);

            UpdateApplied applied = (UpdateApplied) sys.probes.get(TARGET).fishForMessage(
                    Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)),
                    "Update Applied",
                    msg -> msg instanceof UpdateApplied);
            assertEquals(new UpdateApplied(TARGET, i, value), applied);
        }

		// The threshold write engage the replica crash, client-side the write is successfull
        int criticalIndex = threshold - 1;
        int criticalValue = 100 + criticalIndex;
        client.tell(new AbstractClient.WriteRequest(criticalIndex, criticalValue), Actor.noSender());
        WriteResult wrCritical = (WriteResult) clientProbe.fishForMessage(
                Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), "WriteResult",
                msg -> msg instanceof WriteResult);
        assertEquals(new WriteResult(true, criticalIndex, criticalValue, coordinator), wrCritical);
        sys.probes.get(TARGET).expectNoMessage(Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)));

        // Replica_TARGET is not responding (crash confirmed)
        TestKit silenceProbe = new TestKit(sys.system);
        sys.actors.get(TARGET).tell(new Messages.ReadReq(silenceProbe.getRef(), 0), Actor.noSender());
        silenceProbe.expectNoMessage(Duration.ofMillis(sys.client_read_timeout));

        // The system still works without Replica_TARGET
        int survivalIndex = threshold;
        int survivalValue = 100 + survivalIndex;
        client.tell(new AbstractClient.WriteRequest(survivalIndex, survivalValue), Actor.noSender());
        WriteResult wrSurvival = (WriteResult) clientProbe.fishForMessage(
                Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), "WriteResult",
                msg -> msg instanceof WriteResult);
        assertEquals(new WriteResult(true, survivalIndex, survivalValue, coordinator), wrSurvival);

        sys.system.terminate();
    }


	@ParameterizedTest(name = "crash on Replica after N-th WriteOK message => coordinator {0}, nodes {1}")
	@CsvSource({
			"1,7",
			"0,22",
	})
	void crashAfterNthWriteOkMessage(int coordinator, int n_nodes) throws InterruptedException {
		final int TARGET = (coordinator + 1) % n_nodes;
		final int THRESHOLD = 2;
 
		final TestsSystemWrapper sys = TestsCommons.createTestSystem(
				"crashAfterNthWriteOkMessage_" + coordinator + "_" + n_nodes, n_nodes, coordinator);
		TestKit clientProbe = new TestKit(sys.system);
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.ofNullable(sys.actors.get(coordinator)), clientProbe.getRef()),
				"client");
 
		sys.actors.get(TARGET).tell(new Crash(Crash.Type.WriteOK, THRESHOLD), Actor.noSender());
 
		// Replica crashes after applyUpdateIfNeeded:
		// Replica_TARGET applies first THRESHOLD -1 Updates then crashes
		for (int i = 0; i < THRESHOLD; i++) {
			client.tell(new AbstractClient.WriteRequest(i, 100 + i), Actor.noSender());
			WriteResult wr = (WriteResult) clientProbe.fishForMessage(
					Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), "WriteResult",
					msg -> msg instanceof WriteResult);
			// assertTrue(wr.isSuccess);
            assertEquals( new WriteResult(true, i, 100 + i, coordinator), wr);
 
			UpdateApplied applied = (UpdateApplied) sys.probes.get(TARGET).fishForMessage(
					Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)),
					"Update Applied",
					msg -> msg instanceof UpdateApplied);
			assertEquals(new UpdateApplied(TARGET, i, 100 + i), applied);

		}
 
		// Replica_TARGET is crashed thus cannot communicate
		client.tell(new AbstractClient.WriteRequest(2, 100), Actor.noSender());
		WriteResult wr3 = (WriteResult) clientProbe.fishForMessage(
				Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), "WriteResult",
				msg -> msg instanceof WriteResult);
		assertEquals( new WriteResult(true, 2, 100, coordinator), wr3);
		sys.probes.get(TARGET).expectNoMessage(Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)));
 
		sys.system.terminate();
	}
 
	@ParameterizedTest(name = "crash on Replica during election, system still converges => coordinator {0}, nodes {1}")
	@CsvSource({
			"1,7",
			"0,22",
	})
	void crashDuringElectionStillConverges(int coordinator, int n_nodes) throws InterruptedException {
		/*
		 * Replica_TARGET will crash in the middle of the 1st Election
		 * Replica_SURVIVOR is used as a probe
		 */
		final int TARGET = (coordinator + 1) % n_nodes;   
		final int SURVIVOR = (coordinator + 2) % n_nodes; 
 
		final TestsSystemWrapper sys = TestsCommons.createTestSystem(
				"crashDuringElectionStillConverges_" + coordinator + "_" + n_nodes, n_nodes, coordinator);
 
		sys.actors.get(TARGET).tell(new Crash(Crash.Type.Election, 1), Actor.noSender());
 
		sys.actors.get(coordinator).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
 
		ElectionStarted targetStarted = (ElectionStarted) sys.probes.get(TARGET).fishForMessage(
				Duration.ofMillis(TestsCommons.getElectionMaxDelay(sys)),
				"ElectionStarted",
				msg -> msg instanceof ElectionStarted);
		assertEquals(coordinator, targetStarted.crashedCoordinatorId);
 
		CoordinatorElected elected = (CoordinatorElected) sys.probes.get(SURVIVOR).fishForMessage(
				Duration.ofMillis(TestsCommons.getElectionMaxDelay(sys) * 2),
				"CoordinatorElected",
				msg -> msg instanceof CoordinatorElected);
		assertNotEquals(coordinator, elected.newCoordinatorId);
		assertNotEquals(TARGET, elected.newCoordinatorId);
 
		// Final check: the system works fine with the new coordinator
		TestKit clientProbe = new TestKit(sys.system);
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.ofNullable(sys.actors.get(SURVIVOR)), clientProbe.getRef()),
				"client");
		client.tell(new AbstractClient.WriteRequest(0, 77), Actor.noSender());
		WriteResult wr = (WriteResult) clientProbe.fishForMessage(
				Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), "WriteResult",
				msg -> msg instanceof WriteResult);
		assertEquals(new WriteResult(true, 0, 77, SURVIVOR), wr);
 
		sys.system.terminate();
	}

	@ParameterizedTest(name = "crash on Replica after N-th Heartbeat message => coordinator {0}, nodes {1}, threshold {2}")
	@CsvSource({
			"0,7,3",
			"1,22,2",
	})
	void crashAfterNthHeartbeatMessage(int coordinator, int n_nodes, int threshold) throws InterruptedException {
		// TARGET will always be a non-coordinator (because coordinator doesn't receive heartbeats from itself)
		final int TARGET = (coordinator + 1) % n_nodes;

		final TestsSystemWrapper sys = TestsCommons.createTestSystem(
				"crashAfterNthHeartbeatMessage_" + coordinator + "_" + n_nodes + "_" + threshold, n_nodes, coordinator);

		sys.actors.get(TARGET).tell(new Crash(Crash.Type.Heartbeat, threshold), Actor.noSender());

		// Before the threshold-th heartbeat has been processed, Replica_TARGET must still be
		// alive and responsive to ordinary requests (not crased yet)
		Thread.sleep((threshold - 1) * (long) TestsCommons.TEST_COORDINATOR_BEAT_INTERVAL
				+ TestsCommons.getBaseMaxUpdateDelay(sys));

		TestKit aliveProbe = new TestKit(sys.system);
		sys.actors.get(TARGET).tell(new Messages.ReadReq(aliveProbe.getRef(), 0), Actor.noSender());
		aliveProbe.expectMsgClass(Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)), Messages.ReadResp.class);

		// Wait for the threshold-th heartbeat to be delivered and processed: the replica must
		// crash right after handling it
		Thread.sleep(TestsCommons.TEST_COORDINATOR_BEAT_INTERVAL + TestsCommons.getBaseMaxUpdateDelay(sys));

		// Replica_TARGET is not responding anymore (crash confirmed)
		TestKit silenceProbe = new TestKit(sys.system);
		sys.actors.get(TARGET).tell(new Messages.ReadReq(silenceProbe.getRef(), 0), Actor.noSender());
		silenceProbe.expectNoMessage(Duration.ofMillis(sys.client_read_timeout));

		// Final check: the system still works without Replica_TARGET
		TestKit clientProbe = new TestKit(sys.system);
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.ofNullable(sys.actors.get(coordinator)), clientProbe.getRef()),
				"client");
		client.tell(new AbstractClient.WriteRequest(0, 999), Actor.noSender());
		WriteResult wr = (WriteResult) clientProbe.fishForMessage(
				Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), "WriteResult",
				msg -> msg instanceof WriteResult);
		assertEquals(new WriteResult(true, 0, 999, coordinator), wr);

		sys.system.terminate();
	}

}
