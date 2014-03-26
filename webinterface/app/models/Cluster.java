package models;

import models.Database;
import models.ClusterDatabase;
import models.aws.Instance;
import models.aws.AwsCli;

public class Cluster {

    public static void killCluster() {
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

    public static void rebootCluster() {
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

    public static void killSupervisors() {
        Instance[] instances = AwsCli.describeInstances();
        for (Instance i : instances) {
            if ((i.getState() == Instance.STATE_PENDING || i.getState() == Instance.STATE_RUNNING)
                    && i.getRole() == Instance.ROLE_SUPERVISOR) {
                i.terminate();
            }
        }
    }

    public static void rebootSupervisors() {
        Instance[] instances = AwsCli.describeInstances();
        for (Instance i : instances) {
            if ((i.getState() == Instance.STATE_PENDING || i.getState() == Instance.STATE_RUNNING)
                    && i.getRole() == Instance.ROLE_SUPERVISOR) {
                i.restart();
            }
        }
    }

    public static boolean isNimbusUp() {

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