package it.unitn.ds;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.Duration;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

public class Replica extends AbstractReplica {

    // Helper to keep track of pending forward write requests and their timers
    private static class PendingForward {
        public final ActorRef client;
        public final int index;
        public final int value;
        public Cancellable timer;

        public PendingForward(ActorRef client, int index, int value, Cancellable timer) {
            this.client = client;
            this.index = index;
            this.value = value;
            this.timer = timer;
        }
    }

    // Group state (immutable once set)
    private Map<Integer, ActorRef> group;   // id -> ActorRef
    private int coordinatorId;

    // Local state
    private int[] P;
    private int currentEpoch;
    private int nextSeqNum; // Only the coordinator updates it

    // Crash state
    private boolean crashed = false;

    // Election state
    private boolean electionInProgress = false;
    private boolean electionStartedCallbackCalled = false; // Useful to avoid to call callbackOnElectionStarted() multiple times during the same election
    private int nextForwardReqId = 0;

    private final Map<Integer, PendingForward> pendingForwards = new HashMap<>();
    private final List<Messages.WriteReq> bufferedWriteRequests = new ArrayList<>();
    private Cancellable heartbeatTimeoutTimer;

    private final Map<UpdateId, Messages.UpdateWrite> pendingUpdateWrites = new HashMap<>();
    private final Map<UpdateId, Cancellable> updateTimers = new HashMap<>(); 
    private final Map<UpdateId, Set<Integer>> pendingAck = new HashMap<>(); // Coordinator's only

    // Permanent record of every update ever seen, and which ones were applied:
    // needed so a replica can still state which is the most recent update it knows about, 
    // even after that update has already been applied (and thus removed from pendingUpdateWrites),
    // and so the new coordinator can send a full Synchronization to everyone
    private final Map<UpdateId, Messages.UpdateWrite> updateHistory = new HashMap<>();
    private final Set<UpdateId> appliedUpdates = new HashSet<>();

    // Ring election state
    private Map<Integer, UpdateId> electionCandidates; // election message 
    private int electionAckPendingFromId = -1; // recipient of the last election message sent, waiting for its ACK
    private Cancellable electionAckTimer;
    private Cancellable electionOverallTimer; // it guarantees that the election will eventually terminate 

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

    private int getCoordinatorId() {
        return coordinatorId;
    }

    private void unicast(java.io.Serializable msg, ActorRef dest) {
        if (crashed) return;
        tell(msg, dest);
    }

    //
    // ––– INIT –––
    //
    @Override
    public void initSystem(InitSystem sysInit) {
        this.group = sysInit.group;
        this.coordinatorId = sysInit.coordinator_id;
        this.P = new int[POSITIONS_LIST_LENGTH];
        this.currentEpoch = 0;
        this.nextSeqNum = 0;
        debug("system initialized, coordinator=" + coordinatorId + ", N=" + group.size());
        if (isCoordinator()){
            scheduleNextHeartbeat();
        } else {
            resetHeartbeatTimeout();
        }
    }

    //
    // ––– CRASH –––
    //
    @Override
    public void crash(Crash how_to_crash) {
        if (how_to_crash != null && how_to_crash.type == Crash.Type.Now) {
            doCrash();
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
    // –––– Timers –––
    //

    Cancellable setTimeout(int time, java.io.Serializable msg) {
        return getContext().system().scheduler().scheduleOnce(
            Duration.create(time, TimeUnit.MILLISECONDS),  
            getSelf(),
            msg, // the message to send
            getContext().system().dispatcher(), getSelf()
            );

    }

        public void onTimeoutUpdate(Messages.TimeoutUpdate msg) {
            // if the Update is still in the replica waiting to be committed,
            // the WriteOK has never been recieved, therefore the coordinator is crashed
            if (!pendingUpdateWrites.containsKey(msg.id)) return;
            startElection();
        }

        public void onTimeoutForward(Messages.TimeoutForward msg) {
            // if the Update is still in the replica waiting to be committed,
            // the WriteOK has never been recieved, therefore the coordinator is crashed
            if (!pendingForwards.containsKey(msg.localReqId)) return;
            startElection();
        }


    //
    // –––– Heartbeat –––
    //
    private void scheduleNextHeartbeat() {
        setTimeout(getCoordinatorBeatInterval(), new Messages.SendHeartbeat());
    }

    private void onSendHeartbeat(Messages.SendHeartbeat msg) {
        for (ActorRef replica : group.values()) {
            unicast(new Messages.Heartbeat(), replica);
        }
        scheduleNextHeartbeat();
    }

    private void onHeartbeat(Messages.Heartbeat msg) {
        if (isCoordinator()) return; // the coordinator does not expect heartbeats from itself
        resetHeartbeatTimeout();
    }

    private void resetHeartbeatTimeout() {
        if (heartbeatTimeoutTimer != null) heartbeatTimeoutTimer.cancel();
        int detection = (int)(getCoordinatorBeatInterval() * 3.0) + (getMaxLatency() * getSystemNumberOfActors() * 2);
        heartbeatTimeoutTimer = setTimeout(detection, new Messages.SuspectCoordinatorCrashed());
    }

    private void onSuspectCoordinatorCrashed(Messages.SuspectCoordinatorCrashed msg) {
        startElection();
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
        if (electionInProgress) {
            bufferedWriteRequests.add(msg);
            return; // buffer the request until the election is over
        }
        if (isCoordinator()) {
            nextForwardReqId++;
            startUpdate(msg.client, msg.index, msg.value, getSelf(), nextForwardReqId);
        } else {
            nextForwardReqId++;
            int timeout = 2 * getMaxLatency() * getSystemNumberOfActors();
            Cancellable c = setTimeout(timeout, new Messages.TimeoutForward(nextForwardReqId));
            pendingForwards.put(nextForwardReqId, new PendingForward(msg.client, msg.index, msg.value, c));
            unicast(new Messages.ForwardWrite(msg.client, msg.index, msg.value, getSelf(), nextForwardReqId), group.get(coordinatorId));
        }
    }

    // Forward the Write request to the coordinator
    private void onForwardWrite(Messages.ForwardWrite msg) {
        if (!isCoordinator() || electionInProgress) return;
        startUpdate(msg.client, msg.index, msg.value, msg.origin, msg.localReqId);
        //setTimeout(COORDINATOR_BEAT_INTERVAL);
    }

    //
    // ––– UPDATE PROTOCOL: WRITE –––
    //

    // 1.  Upon receiving the update, the coordinator send it to all replicas (its self included)
    private void startUpdate(ActorRef client, int index, int value, ActorRef origin, int localReqId) {
        UpdateId id = new UpdateId(currentEpoch, nextSeqNum++);
        Messages.UpdateWrite update = new Messages.UpdateWrite(id, index, value, origin, client, localReqId);

        pendingAck.put(id, new HashSet<>());

        for (ActorRef replica : group.values()) {
            unicast(update, replica);
        }
    }

    // 2. Every replica save the update and fires an ACK to coordinator
    private void onUpdateWrite(Messages.UpdateWrite msg) {
        if (msg.origin.equals(getSelf()) && pendingForwards.containsKey(msg.localReqId)){
            PendingForward pf = pendingForwards.remove(msg.localReqId);
            if (pf != null && pf.timer != null) pf.timer.cancel();
        }
        pendingUpdateWrites.put(msg.id, msg);
        updateHistory.put(msg.id, msg); // permanent record

        unicast(new Messages.Ack(msg.id, this.id), group.get(coordinatorId));

        int timeout = 2 * getMaxLatency() * getSystemNumberOfActors();
        Cancellable timer = setTimeout(timeout, new Messages.TimeoutUpdate(msg.id));
        updateTimers.put(msg.id, timer);
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
        pendingUpdateWrites.remove(msg.id);
        Cancellable t = updateTimers.remove(msg.id);
        if (t != null) t.cancel();

        applyUpdateIfNeeded(msg.id);
    }

    // Apply the update if it has not been applied yet
    private void applyUpdateIfNeeded(UpdateId id) {
        if (appliedUpdates.contains(id)) return;

        Messages.UpdateWrite update = updateHistory.get(id);
        if (update == null) {
            debug("cannot apply unknown update " + id);
            return;
        }

        P[update.index] = update.value;
        appliedUpdates.add(id);
        callbackOnUpdateApplied(update.index, update.value);
        log("applied update " + id + " (" + update.index + ", " + update.value + ")");
        
        // this replica was the one originally contacted by the client, so it replies directly
        if (update.origin.equals(getSelf())) {
            unicast(new Messages.WriteResp(update.index, update.value, this.id), update.client);
        }
    }

    //
    // ––– COORDINATOR ELECTION (ring-based) –––
    //

    // Given a replica ID, return the next replica in the ring (skipping crashed replicas)
    private int nextInRing(int fromId) {
        List<Integer> ids = new ArrayList<>(group.keySet());
        Collections.sort(ids);
        int idx = ids.indexOf(fromId);
        return ids.get((idx + 1) % ids.size());
    }

    // Return the last known update ID among all replicas
    private UpdateId getLastKnownUpdateId() {
        UpdateId best = new UpdateId(-1, -1);
        for (UpdateId id : updateHistory.keySet()) {
            if (id.compareTo(best) > 0) best = id;
        }
        return best;
    }

    // Callback to notify the application that an election has started (only once per election)
    private void triggerElectionStartedCallback() {
        if (!electionStartedCallbackCalled) {
            electionStartedCallbackCalled = true;
            callbackOnElectionStarted(getCoordinatorId());
        }
    }

    // Start a new election
    private void startElection() {
        if (electionInProgress) return; // ignore if an election is already in progress

        electionInProgress = true;
        triggerElectionStartedCallback();

        electionCandidates = new HashMap<>();
        electionCandidates.put(this.id, getLastKnownUpdateId());

        scheduleElectionOverallTimeout();
        sendElectionTo(nextInRing(this.id), electionCandidates);
    }

    // Send an election message to the next replica in the ring
    private void sendElectionTo(int targetId, Map<Integer, UpdateId> candidates) {
        if (targetId == this.id) {
            // I am the only replica alive, or the ring has completed a full cycle: conclude the election
            concludeElection(candidates);
            return;
        }
        electionAckPendingFromId = targetId;
        unicast(new Messages.Election(this.id, candidates), group.get(targetId));

        int ackTimeoutMs = Math.max(200, 5 * getMaxLatency());
        electionAckTimer = setTimeout(ackTimeoutMs, new Messages.ElectionAckTimeout(targetId));
    }

    private void onElectionAckTimeout(Messages.ElectionAckTimeout msg) {
        if (msg.expectedFromId != electionAckPendingFromId) return; // ignore if the ACK has already been received
        debug("replica " + msg.expectedFromId + " did not respond to election, assuming it crashed");
        sendElectionTo(nextInRing(msg.expectedFromId), electionCandidates);
    }

    private void onElectionAck(Messages.ElectionAck msg) {
        if (electionAckTimer != null) electionAckTimer.cancel();
        electionAckPendingFromId = -1;
    }


    private void onElection(Messages.Election msg) {
        // Always send an ACK to the sender
        unicast(new Messages.ElectionAck(), group.get(msg.senderId));

        // If the election message already contains this replica's ID, it means the ring has completed a full cycle and the election can be concluded
        if (msg.candidates.containsKey(this.id)) {
            concludeElection(msg.candidates);
            return;
        }

        if (!electionInProgress) {
            // TODO: Message from the past (additional check): se non siamo in election e il coordinatore attuale è già tra i candidati di questo messaggio,
            // significa che è un messaggio duplicato rimasto in rete da un'election appena finita
            if (coordinatorId != -1 && msg.candidates.containsKey(coordinatorId)) {
                return;
            }
            electionInProgress = true;
            triggerElectionStartedCallback();
            scheduleElectionOverallTimeout();
        }

        Map<Integer, UpdateId> merged = new HashMap<>(msg.candidates);
        merged.put(this.id, getLastKnownUpdateId());

        electionCandidates = merged;
        sendElectionTo(nextInRing(this.id), merged);
    }

    private void concludeElection(Map<Integer, UpdateId> candidates) {
        int winnerId = pickWinner(candidates);

        if (winnerId == this.id) {
            // I am the winner: I become the new coordinator and notify everyone
            becomeCoordinator();
        } else {
            // I am not the winner, I send the election message to the next replica in the ring
            sendElectionTo(nextInRing(this.id), candidates);
        }
    }

    // Pick the winner based on the highest UpdateId, and in case of a tie, the highest replica ID
    private int pickWinner(Map<Integer, UpdateId> candidates) {
        int winner = -1;
        UpdateId best = null;
        for (Map.Entry<Integer, UpdateId> e : candidates.entrySet()) {
            int candId = e.getKey();
            UpdateId candUpdate = e.getValue();
            if (best == null || candUpdate.compareTo(best) > 0
                    || (candUpdate.compareTo(best) == 0 && candId > winner)) {
                best = candUpdate;
                winner = candId;
            }
        }
        return winner;
    }

    private void becomeCoordinator() {
        if (electionOverallTimer != null) electionOverallTimer.cancel();
        if (electionAckTimer != null) electionAckTimer.cancel();
        electionAckPendingFromId = -1;

        coordinatorId = this.id;
        currentEpoch++;
        nextSeqNum = 0;

        callbackOnCoordinatorElected(this.id);

        completePendingUpdates();
        processPendingForwardsAndBufferedWrites();

        List<Messages.UpdateWrite> allUpdates = new ArrayList<>(updateHistory.values());
        allUpdates.sort(Comparator.comparing(u -> u.id));

        for (ActorRef replica : group.values()) {
            unicast(new Messages.Synchronization(this.id, currentEpoch, allUpdates), replica);
        }

        electionInProgress = false;
        electionStartedCallbackCalled = false;
        scheduleNextHeartbeat();
    }

    private void completePendingUpdates() {
        for (Messages.UpdateWrite u : new ArrayList<>(updateHistory.values())) {
            if (!appliedUpdates.contains(u.id)) {
                startUpdate(u.client, u.index, u.value, u.origin, u.localReqId);
            }
        }
    }

    private void processPendingForwardsAndBufferedWrites() {
        // Process buffered write requests that arrived during the election
        List<Messages.WriteReq> buffered = new ArrayList<>(bufferedWriteRequests);
        bufferedWriteRequests.clear();
        for (Messages.WriteReq req : buffered) {
            onWriteReq(req);
        }

        // Process pending forward requests that arrived during the election
        Map<Integer, PendingForward> pending = new HashMap<>(pendingForwards);
        pendingForwards.clear();

        for (PendingForward pf : pending.values()) {
            if (pf.timer != null) pf.timer.cancel();
            if (isCoordinator()) {
                nextForwardReqId++;
                startUpdate(pf.client, pf.index, pf.value, getSelf(), nextForwardReqId);
            } else {
                nextForwardReqId++;
                int timeout = 2 * getMaxLatency() * getSystemNumberOfActors();
                Cancellable c = setTimeout(timeout, new Messages.TimeoutForward(nextForwardReqId));
                pendingForwards.put(nextForwardReqId, new PendingForward(pf.client, pf.index, pf.value, c));
                unicast(new Messages.ForwardWrite(pf.client, pf.index, pf.value, getSelf(), nextForwardReqId), group.get(coordinatorId));
            }
        }
    }

    private void scheduleElectionOverallTimeout() {
        int margin = (int) (getCoordinatorBeatInterval() * 3.0) + (4 * getMaxLatency() * getSystemNumberOfActors());
        electionOverallTimer = setTimeout(margin, new Messages.ElectionOverallTimeout());
    }

    // Handle the case where the election takes too long and needs to be restarted
    private void onElectionOverallTimeout(Messages.ElectionOverallTimeout msg) {
        if (!electionInProgress) return; // election already concluded
        debug("election taking too long, restarting");
        if (electionAckTimer != null) electionAckTimer.cancel();
        electionAckPendingFromId = -1;
        electionCandidates = null;
        electionInProgress = false;
        startElection();
    }

    //
    // ––– SYNCHRONIZATION –––
    //
    private void onSynchronization(Messages.Synchronization msg) {
        if (electionOverallTimer != null) electionOverallTimer.cancel();
        if (electionAckTimer != null) electionAckTimer.cancel();
        electionAckPendingFromId = -1;

        coordinatorId = msg.newCoordinatorId;
        currentEpoch = msg.newEpoch;

        List<Messages.UpdateWrite> sortedUpdates = new ArrayList<>(msg.updates);
        sortedUpdates.sort(Comparator.comparing(u -> u.id));

        for (Messages.UpdateWrite u : sortedUpdates) {
            updateHistory.putIfAbsent(u.id, u);
            applyUpdateIfNeeded(u.id);
        }

        boolean iAmTheNewCoordinator = (msg.newCoordinatorId == this.id);

        if (!iAmTheNewCoordinator) {
            // Reset the state related to pending forwards and buffered writes, since they are no longer relevant after synchronization
            electionInProgress = false;
            electionStartedCallbackCalled = false;
            callbackOnCoordinatorElected(coordinatorId);
            resetHeartbeatTimeout();
            processPendingForwardsAndBufferedWrites();
        }
    }


    //
    // Receive builder
    //
    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder() 
                .match(Messages.ReadReq.class, this::onReadReq)
                .match(Messages.WriteReq.class, this::onWriteReq)
                .match(Messages.ForwardWrite.class, this::onForwardWrite)
                .match(Messages.UpdateWrite.class, this::onUpdateWrite)
                .match(Messages.Ack.class, this::onAck)
                .match(Messages.WriteOK.class, this::onWriteOK)
                .match(Messages.SendHeartbeat.class, this::onSendHeartbeat)
                .match(Messages.Heartbeat.class, this::onHeartbeat)
                .match(Messages.SuspectCoordinatorCrashed.class, this::onSuspectCoordinatorCrashed)
                .match(Messages.TimeoutUpdate.class, this::onTimeoutUpdate)
                .match(Messages.TimeoutForward.class, this::onTimeoutForward)
                .match(Messages.Election.class, this::onElection)
                .match(Messages.ElectionAck.class, this::onElectionAck)
                .match(Messages.ElectionAckTimeout.class, this::onElectionAckTimeout)
                .match(Messages.ElectionOverallTimeout.class, this::onElectionOverallTimeout)
                .match(Messages.Synchronization.class, this::onSynchronization)
                .build();
    }
}