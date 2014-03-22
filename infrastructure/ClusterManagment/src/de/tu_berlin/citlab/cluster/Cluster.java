package de.tu_berlin.citlab.cluster;

import de.tu_berlin.citlab.cluster.instances.Instance;

public class Cluster {
	private static Cluster INSTANCE = new Cluster();

	private Cluster() {
	}

	public static Cluster getInstance() {
		return INSTANCE;
	}

	public void startNimbus() {
		String keyName = ClusterDatabase.getInstance().getProperty(
				"cluster.key-name");
		String availabilityZone = ClusterDatabase.getInstance().getProperty(
				"cluster.zone");
		String imageId = ClusterDatabase.getInstance().getProperty(
				"nimbus.image-id");
		String instanceType = ClusterDatabase.getInstance().getProperty(
				"nimbus.instance-type");

		AwsCli.runInstance(imageId, instanceType, keyName, availabilityZone);
	}

	public void startCassandra() {
		String keyName = ClusterDatabase.getInstance().getProperty(
				"cluster.key-name");
		String availabilityZone = ClusterDatabase.getInstance().getProperty(
				"cluster.zone");
		String imageId = ClusterDatabase.getInstance().getProperty(
				"cassandra.image-id");
		String instanceType = ClusterDatabase.getInstance().getProperty(
				"cassandra.instance-type");

		AwsCli.runInstance(imageId, instanceType, keyName, availabilityZone);
	}

	public void startSupervisor(int count) {
		String keyName = ClusterDatabase.getInstance().getProperty(
				"cluster.key-name");
		String availabilityZone = ClusterDatabase.getInstance().getProperty(
				"cluster.zone");
		String imageId = ClusterDatabase.getInstance().getProperty(
				"supervisor.image-id");
		String instanceType = ClusterDatabase.getInstance().getProperty(
				"supervisor.instance-type");

		AwsCli.runInstances(imageId, instanceType, keyName, availabilityZone,
				count);
	}

	public void killCluster() {
		Instance[] instances = AwsCli.describeInstances();
		for (Instance i : instances) {
			if ((i.getState() == Instance.STATE_PENDING || i.getState() == Instance.STATE_RUNNING)
					&& (i.getRole() == Instance.ROLE_CASSANDRA
							|| i.getRole() == Instance.ROLE_NIMBUS || i
							.getRole() == Instance.ROLE_SUPERVISOR)) {
				i.terminate();
			}
		}
	}

	public void rebootCluster() {
		Instance[] instances = AwsCli.describeInstances();
		for (Instance i : instances) {
			if ((i.getState() == Instance.STATE_PENDING || i.getState() == Instance.STATE_RUNNING)
					&& (i.getRole() == Instance.ROLE_CASSANDRA
							|| i.getRole() == Instance.ROLE_NIMBUS || i
							.getRole() == Instance.ROLE_SUPERVISOR)) {
				i.restart();
			}
		}
	}

	public void killSupervisors() {
		Instance[] instances = AwsCli.describeInstances();
		for (Instance i : instances) {
			if ((i.getState() == Instance.STATE_PENDING || i.getState() == Instance.STATE_RUNNING)
					&& i.getRole() == Instance.ROLE_SUPERVISOR) {
				i.terminate();
			}
		}
	}

	public void rebootSupervisors() {
		Instance[] instances = AwsCli.describeInstances();
		for (Instance i : instances) {
			if ((i.getState() == Instance.STATE_PENDING || i.getState() == Instance.STATE_RUNNING)
					&& i.getRole() == Instance.ROLE_SUPERVISOR) {
				i.restart();
			}
		}
	}

	public boolean isNimbusUp() {

		for (Instance i : AwsCli.describeInstances()) {

			if (i.getRole() == Instance.ROLE_NIMBUS
					&& i.getState() == Instance.STATE_RUNNING
					&& i.getPublicIp() != null
					&& i.getPublicIp().equals(
							ClusterDatabase.getInstance().getProperty(
									"nimbus.public-ip"))) {
				return true;
			}
		}

		return false;
	}
}
