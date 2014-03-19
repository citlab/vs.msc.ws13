package controllers;

import play.*;
import play.mvc.*;
import play.mvc.Http.*;

import views.html.*;
import models.Cluster;

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
        if(server.equals("cluster")) {
            if(action.equals("reboot")) {
                Cluster.rebootCluster();
            } else if(action.equals("shutdown")) {
                Cluster.killCluster();
            }
        } else if(server.equals("nimbus")) {
            if(action.equals("start")) {
                Cluster.startNimbus();
            } else if (action.equals("stop")) {
                //Cluster.stopNimbus();
            }
        } else if(server.equals("supervisor")) {
            int count = Integer.parseInt(request().getQueryString("n_sv"));
            Cluster.startSupervisor(count);
        } else if(server.equals("cassandra")) {

        } else {
            return notFound("Fehler. Servertyp " + server + " nicht bekannt.");
        }
        return ok("{msg: Server action performed.}");
    }
}
