package models.aws;

import gson.InstancesGson;

public class Instance {
  public static final int PENDING = 0;
  public static final int RUNNING = 16;
  public static final int SHUTTING_DOWN = 32;
  public static final int TERMINATED = 48;
  public static final int STOPPING = 64;
  public static final int STOPPED = 80;

  private final String instanceId;
  private final String imageId;
  private final String instanceType;
  private final String launchTime;
  private String publicIp;
  private State state;

  public enum State {
    PENDING, RUNNING, SHUTTING_DOWN, TERMINATED, STOPPING, STOPPED
  }

  public enum Role {
    NIMBUS, CASSANDRA, ZOOKEEPER, SUPERVISOR, MANAGER, OTHER
  }

  public Instance(InstancesGson instance) {
    this(instance.getInstanceId(), instance.getImageId(), instance
        .getInstanceType(), instance.getState().getName(), instance
        .getPublicIpAddress(), instance.getLaunchTime());
  }

  public Instance(String instanceId, String imageId, String instanceType,
      String state, String publicIp, String launchTime) {
    this.instanceId = instanceId;
    this.imageId = imageId;
    this.instanceType = instanceType;
    this.launchTime = launchTime;
    this.publicIp = publicIp;

    switch (state) {
    case "pending":
      this.state = State.PENDING;
      break;

    case "running":
      this.state = State.RUNNING;
      break;

    case "shutting-down":
      this.state = State.SHUTTING_DOWN;
      break;

    case "terminated":
      this.state = State.TERMINATED;
      break;

    case "stopping":
      this.state = State.STOPPING;
      break;

    case "stopped":
      this.state = State.STOPPED;
      break;

    default:
      break;
    }
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

  public void setPublicIp(String publicIp) {
    this.publicIp = publicIp;
  }

  public String getLaunchTime() {
    return launchTime;
  }

  public State getState() {
    return state;
  }

  public void setState(State state) {
    this.state = state;
  }

  public Role getRole() {
    if (imageId.equals(Config.getInstance().getProperty("nimbus.image-id"))) {
      return Role.NIMBUS;
    } else if (imageId.equals(Config.getInstance().getProperty(
        "supervisor.image-id"))) {
      return Role.SUPERVISOR;
    } else if (imageId.equals(Config.getInstance().getProperty(
        "zookeeper.image-id"))) {
      return Role.ZOOKEEPER;
    } else if (imageId.equals(Config.getInstance().getProperty(
        "cassandra.image-id"))) {
      return Role.CASSANDRA;
    } else {
      return Role.OTHER;
    }
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
