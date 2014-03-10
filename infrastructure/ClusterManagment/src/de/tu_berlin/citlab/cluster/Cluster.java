package de.tu_berlin.citlab.cluster;

import de.tu_berlin.citlab.cluster.Instance.Role;
import de.tu_berlin.citlab.cluster.Instance.State;

public class Cluster {

	public static void startNimbus() {
		String keyName = Config.getInstance().getProperty("cluster.key-name");
		String availabilityZone = Config.getInstance().getProperty(
				"cluster.zone");
		String imageId = Config.getInstance().getProperty("nimbus.image-id");
		String instanceType = Config.getInstance().getProperty(
				"nimbus.instance-type");

		AwsCli.runInstances(imageId, instanceType, keyName, availabilityZone);
	}

	public static void startSupervisor(int count) {
		String keyName = Config.getInstance().getProperty("cluster.key-name");
		String availabilityZone = Config.getInstance().getProperty(
				"cluster.zone");
		String imageId = Config.getInstance()
				.getProperty("supervisor.image-id");
		String instanceType = Config.getInstance().getProperty(
				"supervisor.instance-type");

		AwsCli.runInstances(imageId, instanceType, keyName, availabilityZone,
				count);
	}

	public static void terminateInstance(String instanceId) {
		AwsCli.terminateInstance(instanceId);
	}

	public static void rebootInstance(String instanceId) {
		AwsCli.rebootInstance(instanceId);
	}

	public static void updatePublicIPAddress(String instanceId) {
		String publicIp = Config.getInstance().getProperty("nimbus.public-ip");

		AwsCli.associateAddress(instanceId, publicIp);
	}

	public static void killCluster() {
		Instance[] instances = AwsCli.describeInstances();
		for (Instance i : instances) {
			if ((i.getState() == State.PENDING || i.getState() == State.RUNNING)
					&& (i.getRole() == Role.CASSANDRA
							|| i.getRole() == Role.NIMBUS
							|| i.getRole() == Role.SUPERVISOR || i.getRole() == Role.ZOOKEEPER)) {
				terminateInstance(i.getInstanceId());
			}
		}
	}

	public static void rebootCluster() {
		Instance[] instances = AwsCli.describeInstances();
		for (Instance i : instances) {
			if ((i.getState() == State.PENDING || i.getState() == State.RUNNING)
					&& (i.getRole() == Role.CASSANDRA
							|| i.getRole() == Role.NIMBUS
							|| i.getRole() == Role.SUPERVISOR || i.getRole() == Role.ZOOKEEPER)) {
				rebootInstance(i.getInstanceId());
			}
		}
	}

	public static void killSupervisors() {
		Instance[] instances = AwsCli.describeInstances();
		for (Instance i : instances) {
			if ((i.getState() == State.PENDING || i.getState() == State.RUNNING)
					&& i.getRole() == Role.SUPERVISOR) {
				terminateInstance(i.getInstanceId());
			}
		}
	}

	public static void rebootSupervisors() {
		Instance[] instances = AwsCli.describeInstances();
		for (Instance i : instances) {
			if ((i.getState() == State.PENDING || i.getState() == State.RUNNING)
					&& i.getRole() == Role.SUPERVISOR) {
				rebootInstance(i.getInstanceId());
			}
		}
	}

	public static boolean isNimbusUp() {

		for (Instance i : AwsCli.describeInstances()) {

			if (i.getRole() == Role.NIMBUS
					&& i.getState() == State.RUNNING
					&& i.getPublicIp() != null
					&& i.getPublicIp().equals(
							Config.getInstance()
									.getProperty("nimbus.public-ip"))) {
				return true;
			}
		}

		return false;
	}
}
