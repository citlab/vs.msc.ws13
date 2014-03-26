package controllers;

import play.*;
import play.mvc.*;
import play.mvc.Http.*;

import views.html.*;
import models.aws.Instance;
import models.ClusterDatabase;

public class Server extends Controller {

    public static Result startServer() {
        if(!models.User.loggedIn(false, session("name"), session("session"))) {
            return TODO;
        } else {
            // action is in form 'action_server', for example 'start_nimbus'
            String[] action = request().getQueryString("action").split("_");
            return startServerByAction(action[0], action[1]);
        }
    }

    private static Result startServerByAction(String action, String server) {
        int count = Integer.parseInt(request().getQueryString("n_sv"));
        if(server.equals("cluster")) {
            //handleClusterAction(action);
        } else if(server.equals("nimbus")) {
            handleNimbusAction(action);
        } else if(server.equals("supervisor")) {
            handleSupervisorAction(action, count);
        } else if(server.equals("cassandra")) {
            handleCassandraAction(action);
        } else {
            return notFound("Fehler. Servertyp " + server + " nicht bekannt.");
        }
        return ok("{msg: Server action performed.}");
    }

    private static void handleNimbusAction(String action) {
        Instance inst = null;
        if(action.equals("start")) {
            inst = Instance.createNimbus();
        } else if (action.equals("stop")) {
            //ins = Instance.stopNimbus();
        } else if (action.equals("reboot")) {
            //inst = Instance.rebootNimbus();
        }

        if(inst != null) {
            ClusterDatabase.getInstance().updateInstance(inst);
        }
    }

    private static void handleSupervisorAction(String action, int count) {
        Instance[] inst = null;
        if(action.equals("start")) {
            inst = Instance.createSupervisors(count);
        } else if (action.equals("stop")) {
            //ins = Instance.stopSupervisors();
        } else if (action.equals("reboot")) {
            //inst = Instance.rebootSupervisors();
        }

        if(inst != null) {
            for(Instance i : inst) {
                ClusterDatabase.getInstance().updateInstance(i);
            }
        }
    }

    private static void handleCassandraAction(String action) {
        Instance inst = null;
        if(action.equals("start")) {
            inst = Instance.createCassandra();
        } else if (action.equals("stop")) {
            //ins = Instance.stopCassandra();
        } else if (action.equals("reboot")) {
            //inst = Instance.rebootCassandra();
        }

        if(inst != null) {
            ClusterDatabase.getInstance().updateInstance(inst);
        }
    }
}
