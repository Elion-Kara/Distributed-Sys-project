package it.unitn.ds;

import java.io.Serializable;
import akka.actor.ActorRef;

public class Messages {

    // Client -> Replica

    public static class ReadReq implements Serializable {
        public final ActorRef client;
        public final int index;
        public ReadReq(ActorRef client, int index) {
            this.client = client;
            this.index = index;
        }
    }

    public static class WriteReq implements Serializable {
        public final ActorRef client;
        public final int index;
        public final int value;
        public WriteReq(ActorRef client, int index, int value) {
            this.client = client;
            this.index = index;
            this.value = value;
        }
    }

    // Replica -> Client

    public static class ReadResp implements Serializable {
        public final int index;
        public final Integer value;
        public final int fromReplica;
        public ReadResp(int index, Integer value, int fromReplica) {
            this.index = index;
            this.value = value;
            this.fromReplica = fromReplica;
        }
    }

    public static class WriteResp implements Serializable {
        public final int index;
        public final Integer value;
        public final int fromReplica;
        public WriteResp(int index, Integer value, int fromReplica) {
            this.index = index;
            this.value = value;
            this.fromReplica = fromReplica;
        }
    }

    // Replica (non-coordinator) -> Replica (coordinator)

    public static class ForwardWrite implements Serializable {
        public final ActorRef client;
        public final int index;
        public final int value;
        public final ActorRef origin; // replica that originally received the request from the client
        public ForwardWrite(ActorRef client, int index, int value, ActorRef origin) {
            this.client = client;
            this.index = index;
            this.value = value;
            this.origin = origin;
        }
    }

    public static class WriteDone implements Serializable {
        public final ActorRef client;
        public final int index;
        public final int value;
        public WriteDone(ActorRef client, int index, int value) {
            this.client = client; this.index = index; this.value = value;
        }
    }
}