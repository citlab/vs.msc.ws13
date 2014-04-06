package models.aws;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import play.*;
import play.mvc.*;

import gson.InstancesGson;
import models.Database;
import models.ClusterDatabase;

public class Instance {
  private static final SimpleDateFormat SDF = new SimpleDateFormat(
      "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  public static final int STATE_PENDING = 0;
  public static final int STATE_RUNNING = 16;
  public static final int STATE_SHUTTING_DOWN = 32;
  public static final int STATE_TERMINATED = 48;
  public static final int STATE_STOPPING = 64;
  public static final int STATE_STOPPED = 80;
  public static final int STATE_UNKNOWN = -1;

  public static final int ROLE_NIMBUS = 100;
  public static final int ROLE_CASSANDRA = 101;
  public static final int ROLE_SUPERVISOR = 102;
  public static final int ROLE_OTHER = 103;

  private final String instanceId;
  private final String imageId;
  private final String instanceType;
  private final Date launchTime;
  private final int role;
  private final String publicIp;
  private final int state;

  public static Instance createNimbus() {
    String keyName = ClusterDatabase.getInstance().getProperty(
        "cluster.key-name");
    String availabilityZone = ClusterDatabase.getInstance().getProperty(
        "cluster.zone");
    String imageId = ClusterDatabase.getInstance().getProperty(
        "nimbus.image-id");
    String instanceType = ClusterDatabase.getInstance().getProperty(
        "nimbus.instance-type");

    return AwsCli.runInstance(imageId, instanceType, keyName,
        availabilityZone);
  }

  public static Instance createCassandra() {
    String keyName = ClusterDatabase.getInstance().getProperty(
        "cluster.key-name");
    String availabilityZone = ClusterDatabase.getInstance().getProperty(
        "cluster.zone");
    String imageId = ClusterDatabase.getInstance().getProperty(
        "cassandra.image-id");
    String instanceType = ClusterDatabase.getInstance().getProperty(
        "cassandra.instance-type");

    return AwsCli.runInstance(imageId, instanceType, keyName,
        availabilityZone);
  }

  public static Instance[] createSupervisors(int count) {
    String keyName = ClusterDatabase.getInstance().getProperty(
        "cluster.key-name");
    String availabilityZone = ClusterDatabase.getInstance().getProperty(
        "cluster.zone");
    String imageId = ClusterDatabase.getInstance().getProperty(
        "supervisor.image-id");
    String instanceType = ClusterDatabase.getInstance().getProperty(
        "supervisor.instance-type");

    return AwsCli.runInstances(imageId, instanceType, keyName,
        availabilityZone, count);
  }

  public static Instance createInstance(InstancesGson instance) {
    return new Instance(instance);
  }

  private Instance(InstancesGson instance) {
    this(instance.getInstanceId(), instance.getImageId(), instance
        .getInstanceType(), instance.getState().getName(), instance
        .getPublicIpAddress(), instance.getLaunchTime());
  }

  private Instance(String instanceId, String imageId, String instanceType,
      String state, String publicIp, String launchTime) {
    this.instanceId = instanceId;
    this.imageId = imageId;
    this.instanceType = instanceType;

    Date date = null;
    try {
      date = SDF.parse(launchTime);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    this.launchTime = date;

    this.publicIp = publicIp;

    if (imageId.equals(ClusterDatabase.getInstance().getProperty(
        "nimbus.image-id"))) {
      this.role = ROLE_NIMBUS;
    } else if (imageId.equals(ClusterDatabase.getInstance().getProperty(
        "supervisor.image-id"))) {
      this.role = ROLE_SUPERVISOR;
    } else if (imageId.equals(ClusterDatabase.getInstance().getProperty(
        "cassandra.image-id"))) {
      this.role = ROLE_CASSANDRA;
    } else {
      this.role = ROLE_OTHER;
    }

    switch (state) {
    case "pending":
      this.state = STATE_PENDING;
      break;

    case "running":
      this.state = STATE_RUNNING;
      break;

    case "shutting-down":
      this.state = STATE_SHUTTING_DOWN;
      break;

    case "terminated":
      this.state = STATE_TERMINATED;
      break;

    case "stopping":
      this.state = STATE_STOPPING;
      break;

    case "stopped":
      this.state = STATE_STOPPED;
      break;

    default:
      this.state = STATE_UNKNOWN;
      break;
    }
  }

  public void terminate() {
    AwsCli.terminateInstance(instanceId);
  }

  public void restart() {
    AwsCli.rebootInstance(instanceId);
  }

  public String getInstanceId() {
    return instanceId;
  }

  public String getImageId() {
    return imageId;
  }

  public String getInstanceType() {
    return instanceType;
  }

  public String getPublicIp() {
    return publicIp;
  }

  public Date getLaunchTime() {
    return launchTime;
  }

  public int getState() {
    return state;
  }

  public int getRole() {
    return this.role;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    if (this.instanceId == ((Instance) obj).instanceId) {
      return true;
    }

    return false;
  }

  @Override
  public String toString() {
    return imageId + "\t" + instanceId + "\t" + instanceType + "\t" + state;
  }
}