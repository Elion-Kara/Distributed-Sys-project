package it.unitn.ds;

import java.util.Map;
import java.util.Optional;
import akka.actor.ActorRef;
import akka.actor.Props;

public class Replica extends AbstractReplica {

    // Group state (immutable once set)
    private Map<Integer, ActorRef> group;   // id -> ActorRef
    private int coordinatorId;

    // Local state
    private int[] P;
    private int currentEpoch; // TODO: this counter will become dynamic with election implementation (for now it's always 0)
    private int nextSeqNum; // used only if this replica is the coordinator

    // Crash state
    private boolean crashed = false;

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    @Override
    public int getSystemNumberOfActors() {
        return group.size();
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        this.group = sysInit.group;
        this.coordinatorId = sysInit.coordinator_id;
        this.P = new int[POSITIONS_LIST_LENGTH]; // initialized to 0
        this.currentEpoch = 0;
        this.nextSeqNum = 0;
        debug("system initialized, coordinator=" + coordinatorId + ", N=" + group.size());
    }

    @Override
    public void crash(Crash how_to_crash) {
        if (how_to_crash.type == Crash.Type.Now) {
            doCrash();
        } else {
            // TODO: implement delayed crash (after N messages of a certain type)
            debug("scheduled crash requested (" + how_to_crash.type + ", "
                    + how_to_crash.after_n_messages_of_type + ") - not yet implemented");
        }
    }

    private void doCrash() {
        this.crashed = true;
        getContext().become(crashedReceive());
    }

    private Receive crashedReceive() {
        // In crashed state, the replica ignores all messages
        return receiveBuilder().matchAny(msg -> {}).build();
    }

    // Helper methods

    private boolean isCoordinator() {
        return coordinatorId == this.id;
    }

    private void unicast(java.io.Serializable msg, ActorRef dest) {
        if (crashed) return;
        tell(msg, dest); // tell() is a method from AbstractReplica that simulates network latency
    }

    // READ and WRITE operations

    private void onReadReq(Messages.ReadReq msg) {
        int value = P[msg.index];
        unicast(new Messages.ReadResp(msg.index, value, this.id), msg.client);
    }

    private void onWriteReq(Messages.WriteReq msg) {
        if (isCoordinator()) {
            applyAndRespond(msg.client, msg.index, msg.value, getSelf());
        } else {
            unicast(new Messages.ForwardWrite(msg.client, msg.index, msg.value, getSelf()), group.get(coordinatorId));
        }
    }

    private void onForwardWrite(Messages.ForwardWrite msg) {
        if (!isCoordinator()) {
            debug("received ForwardWrite but I'm not coordinator, ignoring");
            return;
        }
        applyAndRespond(msg.client, msg.index, msg.value, msg.origin);
    }

    private void applyAndRespond(ActorRef client, int index, int value, ActorRef origin) {
        UpdateId id = new UpdateId(currentEpoch, nextSeqNum++);

        P[index] = value;
        callbackOnUpdateApplied(index, value);
        log("applied update " + id + " (" + index + ", " + value + ")");

        if (origin.equals(getSelf())) {
            // the coordinator received the request directly from the client, so it can respond directly
            unicast(new Messages.WriteResp(index, value, this.id), client);
        } else {
            // the coordinator received the request forwarded by another replica, so it will respond to that replica,
            // which will then respond to the client
            unicast(new Messages.WriteDone(client, index, value), origin);
        }
    }

    private void onWriteDone(Messages.WriteDone msg) {
        unicast(new Messages.WriteResp(msg.index, msg.value, this.id), msg.client);
    }

    // Receive

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(Messages.ReadReq.class, this::onReadReq)
                .match(Messages.WriteReq.class, this::onWriteReq)
                .match(Messages.ForwardWrite.class, this::onForwardWrite)
                .match(Messages.WriteDone.class, this::onWriteDone)
                .build();
    }
}