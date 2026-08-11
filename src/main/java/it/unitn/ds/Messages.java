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

    // Replica (non-coordinator) -> Replica (coordinator): 1st phase of Write committing
    public static class ForwardWrite implements Serializable {
        public final ActorRef client;
        public final int index;
        public final int value;
        public final ActorRef origin; // replica that originally received the request from the client
        public final int localReqId;
        public ForwardWrite(ActorRef client, int index, int value, ActorRef origin, int localReqId) {
            this.client = client;
            this.index = index;
            this.value = value;
            this.origin = origin;
            this.localReqId = localReqId;
        
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

    // Coordinator -> Replicas (all of them, including itself)
    public static class UpdateWrite implements Serializable{
        public final UpdateId id;
        public final int index;
        public final int value;
        public final ActorRef origin;
        public final ActorRef client;
        public final int localReqId;
        public UpdateWrite(UpdateId id, int index, int value, ActorRef origin, ActorRef client, int localReqId) {
            this.id = id;
            this.index = index;
            this.value = value;
            this.origin = origin;
            this.client = client;
            this.localReqId = localReqId;
        }
    }

    // Replica -> Coordinator : acknowledgment of a proposed Update
    public static class Ack implements Serializable {
        public final UpdateId id;
        public final int fromReplica;
        public Ack(UpdateId id, int fromReplica) {
            this.id = id;
            this.fromReplica = fromReplica;
        }
    }

    // Replica (non-coordinator) -> Replica (coordinator): 2st phase of Write committing
    public static class WriteOK implements Serializable {
        public final UpdateId id;
        public WriteOK(UpdateId id) {
            this.id = id;
        }
    }

    public static class Timeout implements Serializable {}
    
    public static class TimeoutUpdate implements Serializable {
        public final UpdateId id;
        public TimeoutUpdate(UpdateId id){
            this.id = id;
        }
    }

    public static class TimeoutForward implements Serializable {
        public final int localReqId;
        public TimeoutForward(int localReqId){
            this.localReqId = localReqId;
        }
    }

    public static class Heartbeat implements Serializable {}

    public static class SendHeartbeat implements Serializable {}

    public static class SuspectCoordinatorCrashed implements Serializable {}

    
}