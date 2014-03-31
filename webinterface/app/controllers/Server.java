package controllers;

import play.*;
import play.mvc.*;
import play.mvc.Http.*;

import views.html.*;
import models.aws.Instance;
import models.ClusterDatabase;
import models.Nimbus;
import models.Cassandra;
import models.Supervisor;

import play.libs.Json;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Server extends Controller {

    @BodyParser.Of(BodyParser.Json.class)
    public static Result updateServerData() {
        ObjectNode result = Json.newObject();
        result.put("Nimbus", Server.updateStatusFor(new Nimbus()));
        result.put("Cassandra", Server.updateStatusFor(new Cassandra()));
        result.put("Supervisor", Server.updateStatusFor(new Supervisor()));
        return ok(result);
    }

    private static ObjectNode updateStatusFor(models.Server server) {
        ObjectNode result = Json.newObject();
        result.put("ip", server.getIp());
        result.put("status", server.getStatus());
        //result.put("aws", server.getInstanceData());
        return result;
    }

    public static Result startNimbus() {
        return ok("started");
    }

    public static Result stopNimbus() {
        return ok("stoped");
    }

    public static Result rebootNimbus() {
        return ok("rebooted");
    }

    public static Result startCassandra() {
        return ok("started");
    }

    public static Result stopCassandra() {
        return ok("stoped");
    }

    public static Result rebootCassandra() {
        return ok("rebooted");
    }

    public static Result startSupervisor(int count) {
        return ok("started" + count + "Supervisors");
    }

    public static Result stopSupervisor() {
        return ok("stoped");
    }

    public static Result rebootSupervisor() {
        return ok("rebooted");
    }
}
