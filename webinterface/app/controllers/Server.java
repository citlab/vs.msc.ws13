package controllers;

import play.*;
import play.mvc.*;
import play.mvc.Http.*;

import views.html.*;
import models.aws.Instance;
import models.aws.AwsCli;
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
        Instance.createNimbus();
        return ok("started nimbus");
    }

    public static Result stopNimbus() {
        return ok("stoped nimbus");
    }

    public static Result rebootNimbus() {
        return ok("rebooted nimbus");
    }

    public static Result startCassandra() {
        Instance.createCassandra();
        return ok("started cassandra");
    }

    public static Result stopCassandra() {
        return ok("stoped cassandra");
    }

    public static Result rebootCassandra() {
        return ok("rebooted cassandra");
    }

    public static Result startSupervisor(int count) {
        Instance.createSupervisors(count);
        return ok("started" + count + "Supervisors");
    }

    public static Result stopSupervisor() {
        return ok("stoped supervisor");
    }

    public static Result rebootSupervisor() {
        return ok("rebooted supervisor");
    }
}
