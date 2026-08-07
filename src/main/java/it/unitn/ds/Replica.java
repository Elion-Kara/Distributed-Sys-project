package it.unitn.ds;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import akka.actor.ActorRef;
import akka.actor.Props;

public class Replica extends AbstractReplica {

    // Group state (immutable once set)
    private Map<Integer, ActorRef> group;   // id -> ActorRef
    private int coordinatorId;

    // Local state
    private int[] P;
    private int currentEpoch; // TODO: this counter will become dynamic with election implementation (for now it's always 0)
    private int nextSeqNum; // Only the coordinator updates it

    // Crash state
    private boolean crashed = false;

    private final Map<UpdateId, Messages.UpdateWrite> pendingUpdateWrites = new HashMap<>();
    private final Map<UpdateId, Set<Integer>> pendingAck = new HashMap<>(); // Coordinator's only


    // Constructors
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

    //
    // ––– Helper methods ––––
    //
    @Override
    public int getSystemNumberOfActors() {
        return group.size();
    }

    private boolean isCoordinator() {
        return coordinatorId == this.id;
    }

    private void unicast(java.io.Serializable msg, ActorRef dest) {
        if (crashed) return;
        tell(msg, dest); // tell() is a method from AbstractReplica that simulates network latency
    }

    //
    // ––– INIT –––
    //
    @Override
    public void initSystem(InitSystem sysInit) {
        this.group = sysInit.group;
        this.coordinatorId = sysInit.coordinator_id;
        this.P = new int[POSITIONS_LIST_LENGTH]; // initialized to 0
        this.currentEpoch = 0;
        this.nextSeqNum = 0;
        debug("system initialized, coordinator=" + coordinatorId + ", N=" + group.size());
    }

    //
    // ––– CRASH –––
    //
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

    //
    // ––– READ –––
    //

    private void onReadReq(Messages.ReadReq msg) {
        int value = P[msg.index];
        unicast(new Messages.ReadResp(msg.index, value, this.id), msg.client);
    }

    //
    // ––– WRITE –––
    //

    // On Write Request start the Update protocol
    private void onWriteReq(Messages.WriteReq msg) {
        if (isCoordinator()) {
            startUpdate(msg.client, msg.index, msg.value, getSelf());
        } else {
            // Forward the Write request to the coordinator
            unicast(new Messages.ForwardWrite(msg.client, msg.index, msg.value, getSelf()), group.get(coordinatorId));
        }
    }

    // Forward the Write request to the coordinator
    private void onForwardWrite(Messages.ForwardWrite msg) {
        if (!isCoordinator()) {
            debug("received ForwardWrite but I'm not coordinator, ignoring");
            return;
        }
        startUpdate(msg.client, msg.index, msg.value, msg.origin);
    }

    // Notify the Client of the Write response
    private void onWriteDone(Messages.WriteDone msg) {
        unicast(new Messages.WriteResp(msg.index, msg.value, this.id), msg.client);
    }

    //
    // ––– UPDATE PROTOCOL: WRITE –––
    //

    // 1.  Upon receiving the update, the coordinator send it to all replicas (its self included)
    private void startUpdate(ActorRef client, int index, int value, ActorRef origin) {
        UpdateId id = new UpdateId(currentEpoch, nextSeqNum++);
        Messages.UpdateWrite update = new Messages.UpdateWrite(id, index, value, origin, client);

        pendingAck.put(id, new HashSet<>());

        for (ActorRef replica : group.values()) {
            unicast(update, replica);
        }
    }

    // 2. Every replica save the update and fires an ACK to coordinator
    private void onUpdateWrite(Messages.UpdateWrite msg) {
        pendingUpdateWrites.put(msg.id, msg);
        unicast(new Messages.Ack(msg.id, this.id), group.get(coordinatorId));
    }

    // 3. Coordinator gets the ACKs and applies the update if quorum is reached
    private void onAck(Messages.Ack msg) {
        if (!isCoordinator()) return;

        Set<Integer> acked = pendingAck.get(msg.id);
        if (acked == null) return;

        acked.add(msg.fromReplica);

        int quorum = (group.size() / 2) + 1;
        if (acked.size() >= quorum) {
            pendingAck.remove(msg.id);

            for (ActorRef replica : group.values()){
                unicast(new Messages.WriteOK(msg.id), replica);
            }
        }
    }

    // 4. Every replica commits the write updates
    private void onWriteOK(Messages.WriteOK msg) {
        Messages.UpdateWrite update = pendingUpdateWrites.remove(msg.id);
        if (update == null) {
            debug("received WriteOk for unknown/duplicate update " + msg.id + ", ignoring");
            return;
        }

        P[update.index] = update.value;
        callbackOnUpdateApplied(update.index, update.value);
        log("applied update " + msg.id + " (" + update.index + ", " + update.value + ")");

        // this replica was the one originally contacted by the client, so it replies directly
        if (update.origin.equals(getSelf())){
            unicast(new Messages.WriteResp(update.index, update.value, this.id), update.client);
        } else if (isCoordinator()) {
            unicast(new Messages.WriteDone(update.client, update.index, update.value), update.origin);
        }

    }

    //
    //
    //


    //
    // Receive builder
    //
    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder() 
                .match(Messages.ReadReq.class, this::onReadReq)
                .match(Messages.WriteReq.class, this::onWriteReq)
                .match(Messages.ForwardWrite.class, this::onForwardWrite)
                .match(Messages.WriteDone.class, this::onWriteDone)
                .match(Messages.UpdateWrite.class, this::onUpdateWrite)
                .match(Messages.Ack.class, this::onAck)
                .match(Messages.WriteOK.class, this::onWriteOK)
                .build();
    }
}