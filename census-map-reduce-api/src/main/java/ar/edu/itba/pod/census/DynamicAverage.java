package ar.edu.itba.pod.census;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Created by santi698 on 22/11/16.
 */
public class DynamicAverage implements DataSerializable {
    private long sum;
    private long amount;

    public DynamicAverage(final long sum, final long amount) {
        this.sum = sum;
        this.amount = amount;
    }

    public DynamicAverage() {
        this(0, 0);
    }

    public long getSum() {
        return sum;
    }

    public long getAmount() {
        return amount;
    }

    public synchronized DynamicAverage add(final DynamicAverage dynamicAverage) {
        return new DynamicAverage(this.sum + dynamicAverage.getSum(), this.amount + dynamicAverage.getAmount());
    }

    public synchronized DynamicAverage addValue(final long value) {
        return new DynamicAverage(this.sum + value, this.amount + 1);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DynamicAverage that = (DynamicAverage) o;

        if (sum != that.sum) return false;
        return amount == that.amount;

    }

    @Override
    public int hashCode() {
        int result = (int) (sum ^ (sum >>> 32));
        result = 31 * result + (int) (amount ^ (amount >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "DynamicAverage{" +
                "sum=" + sum +
                ", amount=" + amount +
                '}';
    }

    public Float getFloatValue() {
        if (amount == 0) return 0f;
        return sum / (float) amount;
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        out.writeLong(sum);
        out.writeLong(amount);
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        sum = in.readLong();
        amount = in.readLong();
    }
}
