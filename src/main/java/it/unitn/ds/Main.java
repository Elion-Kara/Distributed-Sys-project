package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractClient.ReadRequest;
import it.unitn.ds.AbstractClient.WriteRequest;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.InitSystem;

public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("START");
        System.out.println("========================================\n");

        final int N_REPLICAS = 5;
        final int COORDINATOR_ID = 0;
        final long READ_TIMEOUT = 3000;
        final long WRITE_TIMEOUT = 5000;

        final ActorSystem system = ActorSystem.create("TestMain");

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);

        Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.put(i,
                system.actorOf(
                    Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                    "Replica_" + i
                )
            );
        }

        InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            entry.getValue().tell(initMsg, ActorRef.noSender());
        }

        Thread.sleep(200); // time to process InitSystem messages and elect the initial coordinator

        // Client linked to the initial coordinator (Replica_0)
        ActorRef client = system.actorOf(
            Client.props(READ_TIMEOUT, WRITE_TIMEOUT, Optional.of(replicas.get(COORDINATOR_ID))),
            "Client_Demo"
        );



        section("1) Direct WRITE on coordinator (Replica_0)");

        client.tell(new WriteRequest(0, 10), ActorRef.noSender());
        Thread.sleep(800);



        section("2) READ on coordinator (Replica_0): should return the value just written");

        client.tell(new ReadRequest(0), ActorRef.noSender());
        Thread.sleep(500);



        section("3) WRITE on a NON coordinator replica (Replica_2): test the forward");

        ActorRef client2 = system.actorOf(
            Client.props(READ_TIMEOUT, WRITE_TIMEOUT, Optional.of(replicas.get(2))),
            "NonCoord_Client"
        );
        client2.tell(new WriteRequest(1, 55), ActorRef.noSender());
        Thread.sleep(800);



        section("4) READ on all replicas (including the non-coordinator) to verify the value is consistent");

        for (int i = 0; i < N_REPLICAS; i++) {
            ActorRef reader = system.actorOf(
                Client.props(READ_TIMEOUT, WRITE_TIMEOUT, Optional.of(replicas.get(i))),
                "Client_Reader_" + i
            );
            reader.tell(new ReadRequest(1), ActorRef.noSender());
            Thread.sleep(300);
        }



        section("5) CRASH of the coordinator (Replica_0): wait for a new election");

        replicas.get(COORDINATOR_ID).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
        System.out.println(">>> Replica_0 (coordinator) has crashed.");
        System.out.println(">>> Waiting for a new election...\n");
        Thread.sleep(4000); // time for the election to complete and a new coordinator to be elected



        section("6) WRITE after the election: test that the new coordinator is working");

        // client2 is pointing to Replica_2, which is now the new coordinator after the election
        client2.tell(new WriteRequest(2, 99), ActorRef.noSender());
        Thread.sleep(800);



        section("7) READ on all replicas to verify the value is consistent after the election");

        for (int i = 0; i < N_REPLICAS; i++) {
            if (i == COORDINATOR_ID) continue; // crashed, skip
            ActorRef reader = system.actorOf(
                Client.props(READ_TIMEOUT, WRITE_TIMEOUT, Optional.of(replicas.get(i))),
                "Client_FinalCheck_" + i
            );
            reader.tell(new ReadRequest(2), ActorRef.noSender());
            Thread.sleep(300);
        }

        Thread.sleep(500);

        system.terminate();

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }

    private static void section(String title) {
        System.out.println("\n----------------------------------------");
        System.out.println(title);
        System.out.println("----------------------------------------");
    }
}