package de.tu_berlin.citlab.cluster;

public class Cluster {
	private static Cluster INSTANCE = new Cluster();

	private Cluster() {
	}

	public static Cluster getInstance() {
		return INSTANCE;
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
