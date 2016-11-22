package ar.edu.itba.pod.census;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Created by santi698 on 22/11/16.
 */
public class HomeTypeData implements DataSerializable {
    private Integer homeType;
    private Set<Long> uniqueHomes;
    private AtomicLong inhabitantsCount;

    public HomeTypeData(final Integer homeType) {
        this.uniqueHomes = new CopyOnWriteArraySet<>();
        this.homeType = homeType;
        this.inhabitantsCount = new AtomicLong()
        ;
    }

    public int getHomeType() {
        return homeType;
    }

    public Set<Long> getUniqueHomes() {
        return uniqueHomes;
    }

    public long getInhabitantsCount() {
        return inhabitantsCount.get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HomeTypeData that = (HomeTypeData) o;

        return homeType == that.homeType;

    }

    @Override
    public int hashCode() {
        return homeType;
    }

    @Override
    public synchronized String toString() {
        return "HomeTypeData{" +
                "homeType=" + homeType +
                ", uniqueHomesCount=" + uniqueHomes.size() +
                ", inhabitantsCount=" + inhabitantsCount +
                '}';
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        final long[] uniqueHomesArray = uniqueHomes.stream().mapToLong(Number::longValue).toArray();
        out.writeInt(homeType);
        out.writeLong(inhabitantsCount.get());
        out.writeLongArray(uniqueHomesArray);
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        homeType = in.readInt();
        inhabitantsCount = new AtomicLong(in.readLong());
        uniqueHomes = new CopyOnWriteArraySet<>(Arrays.stream(in.readLongArray()).boxed().collect(Collectors.toSet()));
    }

    public void addInhabitant(final Long homeId) {
        uniqueHomes.add(homeId);
        inhabitantsCount.incrementAndGet();
    }

    public void combine(final HomeTypeData homeTypeData) {
        this.inhabitantsCount.addAndGet(homeTypeData.getInhabitantsCount());
        this.uniqueHomes.addAll(homeTypeData.getUniqueHomes());
    }

    public Float getAverageInhabitants() {
        if (uniqueHomes.size() == 0) { return 0f; }
        return inhabitantsCount.get() / (float) uniqueHomes.size();
    }
}
