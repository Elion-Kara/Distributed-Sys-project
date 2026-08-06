package it.unitn.ds;

import java.io.Serializable;

// Class representing the unique identifier for an update in the system, consisting of an epoch and a sequence number (<e,i>)

public final class UpdateId implements Serializable, Comparable<UpdateId> {
    public final int epoch;
    public final int seqNum;

    public UpdateId(int epoch, int seqNum) {
        this.epoch = epoch;
        this.seqNum = seqNum;
    }

    @Override
    public int compareTo(UpdateId other) {
        if (this.epoch != other.epoch) return Integer.compare(this.epoch, other.epoch);
        return Integer.compare(this.seqNum, other.seqNum);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof UpdateId)) return false;
        UpdateId u = (UpdateId) o;
        return epoch == u.epoch && seqNum == u.seqNum;
    }

    @Override
    public int hashCode() {
        return 31 * epoch + seqNum;
    }

    @Override
    public String toString() {
        return epoch + ":" + seqNum;
    }
}