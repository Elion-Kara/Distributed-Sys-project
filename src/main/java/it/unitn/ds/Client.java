package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

public class Client extends AbstractClient {

    // Assumption: at most one pending request per index at a time (key = index).
    // If handling concurrent requests on the same index is required, a composite key
    // should be used instead
    private final Map<Integer, Cancellable> pendingReadTimeouts = new HashMap<>();
    private final Map<Integer, Cancellable> pendingWriteTimeouts = new HashMap<>();

    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
    }

    @Override
    public void sendRead(ActorRef replica, int index) {
        log("requesting READ (" + index + ") to " + replica.path().name());

        Cancellable timeout = getContext().system().scheduler().scheduleOnce(
                Duration.create(getReadTimeoutDelay(), TimeUnit.MILLISECONDS),
                getSelf(),
                new ReadTimeout(getSelf(), replica, index),
                getContext().system().dispatcher(),
                getSelf());
        pendingReadTimeouts.put(index, timeout);

        replica.tell(new Messages.ReadReq(getSelf(), index), getSelf());
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        log("requesting WRITE (" + index + ", " + value + ") to " + replica.path().name());

        Cancellable timeout = getContext().system().scheduler().scheduleOnce(
                Duration.create(getWriteTimeoutDelay(), TimeUnit.MILLISECONDS),
                getSelf(),
                new WriteTimeout(getSelf(), replica, index, value),
                getContext().system().dispatcher(),
                getSelf());
        pendingWriteTimeouts.put(index, timeout);

        replica.tell(new Messages.WriteReq(getSelf(), index, value), getSelf());
    }

    private void onReadResp(Messages.ReadResp msg) {
        Cancellable t = pendingReadTimeouts.remove(msg.index);
        if (t == null) return; // timeot already passed: the response is too late (we ignore it)
        t.cancel();
        callbackOnReadResult(new ReadResult(msg.value != null, msg.index, msg.value, msg.fromReplica));
    }

    private void onWriteResp(Messages.WriteResp msg) {
        Cancellable t = pendingWriteTimeouts.remove(msg.index);
        if (t == null) return;
        t.cancel();
        callbackOnWriteResult(new WriteResult(msg.value != null, msg.index, msg.value, msg.fromReplica));
    }

    private void onReadTimeout(ReadTimeout timeout) {
        if (pendingReadTimeouts.remove(timeout.index) == null) return; // response already arrived
        callbackOnReadTimeout(timeout);
    }

    private void onWriteTimeout(WriteTimeout timeout) {
        if (pendingWriteTimeouts.remove(timeout.index) == null) return;
        callbackOnWriteTimeout(timeout);
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(Messages.ReadResp.class, this::onReadResp)
                .match(Messages.WriteResp.class, this::onWriteResp)
                .match(ReadTimeout.class, this::onReadTimeout)
                .match(WriteTimeout.class, this::onWriteTimeout)
                .build();
    }
}