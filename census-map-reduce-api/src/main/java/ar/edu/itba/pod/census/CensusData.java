/*
 * (c) 2003-2014 MuleSoft, Inc. This software is protected under international copyright
 * law. All use of this software is subject to MuleSoft's Master Subscription Agreement
 * (or other master license agreement) separately entered into in writing between you and
 * MuleSoft. If such an agreement is not in place, you may not use the software.
 */

package ar.edu.itba.pod.census;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class CensusData implements DataSerializable {

    private int homeType;

    private int serviceQuality;

    private int gender;

    private int age;

    private int literacy;

    private int activity;

    private String departmentName;

    private String provinceName;

    private long homeId;

    public int getHomeType() {
        return homeType;
    }

    public void setHomeType(int homeType) {
        this.homeType = homeType;
    }

    public int getServiceQuality() {
        return serviceQuality;
    }

    public void setServiceQuality(int serviceQuality) {
        this.serviceQuality = serviceQuality;
    }

    public int getGender() {
        return gender;
    }

    public void setGender(int gender) {
        this.gender = gender;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getLiteracy() {
        return literacy;
    }

    public void setLiteracy(int literacy) {
        this.literacy = literacy;
    }

    public int getActivity() {
        return activity;
    }

    public void setActivity(int activity) {
        this.activity = activity;
    }

    public String getDepartmentName() {
        return departmentName;
    }

    public void setDepartmentName(String departmentName) {
        this.departmentName = departmentName;
    }

    public String getProvinceName() {
        return provinceName;
    }

    public void setProvinceName(String provinceName) {
        this.provinceName = provinceName;
    }

    public long getHomeId() {
        return homeId;
    }

    public void setHomeId(long homeId) {
        this.homeId = homeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof CensusData))
            return false;

        CensusData that = (CensusData) o;

        if (homeType != that.homeType)
            return false;
        if (serviceQuality != that.serviceQuality)
            return false;
        if (gender != that.gender)
            return false;
        if (age != that.age)
            return false;
        if (literacy != that.literacy)
            return false;
        if (activity != that.activity)
            return false;
        if (homeId != that.homeId)
            return false;
        if (departmentName != null ? !departmentName.equals(that.departmentName) : that.departmentName != null)
            return false;
        return provinceName != null ? provinceName.equals(that.provinceName) : that.provinceName == null;

    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        out.writeInt(homeType);
        out.writeInt(serviceQuality);
        out.writeInt(gender);
        out.writeInt(age);
        out.writeInt(literacy);
        out.writeInt(activity);
        out.writeUTF(departmentName);
        out.writeUTF(provinceName);
        out.writeLong(homeId);
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        homeType = in.readInt();
        serviceQuality = in.readInt();
        gender = in.readInt();
        age = in.readInt();
        literacy = in.readInt();
        activity = in.readInt();
        departmentName = in.readUTF();
        provinceName = in.readUTF();
        homeId = in.readLong();
    }

    @Override
    public int hashCode() {
        int result = homeType;
        result = 31 * result + serviceQuality;
        result = 31 * result + gender;
        result = 31 * result + age;
        result = 31 * result + literacy;
        result = 31 * result + activity;
        result = 31 * result + (departmentName != null ? departmentName.hashCode() : 0);
        result = 31 * result + (provinceName != null ? provinceName.hashCode() : 0);
        result = 31 * result + (int) (homeId ^ (homeId >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "CensusData{" +
                "homeType=" + homeType +
                ", serviceQuality=" + serviceQuality +
                ", gender=" + gender +
                ", age=" + age +
                ", literacy=" + literacy +
                ", activity=" + activity +
                ", departmentName='" + departmentName + '\'' +
                ", provinceName='" + provinceName + '\'' +
                ", homeId=" + homeId +
                '}';
    }
}
