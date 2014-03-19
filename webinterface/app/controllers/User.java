package controllers;

import play.*;
import play.mvc.*;
import play.mvc.Http.*;

import views.html.*;

import models.Cluster;
import models.Database;

import java.util.Map;

public class User extends Controller {

    public static Result user() {
        if(models.User.isAdmin(session("name"))) {
            return ok(user.render());
        } else {
            return redirect("/");
        }
    }

    public static Result create() {
        final Map<String, String[]> values = request().body().asFormUrlEncoded();
        String name = values.get("name")[0];
        String password = values.get("password")[0];

        if(Database.getInstance().doesUserExist(name) || password == "" || password == null) return notFound("{msg: user exists or password is empty.}");

        Database.getInstance().createUser(name, password);
        return ok("{msg: Neuer User " + name + " wurde angelegt}");
    }

    public static Result login() {
        final Map<String, String[]> values = request().body().asFormUrlEncoded();
        String name = values.get("name")[0];
        String password = values.get("password")[0];
        String sessionId = new Long(System.currentTimeMillis()/1000).toString();

        if(Database.getInstance().validateUser(name, password)) {
            session("name", name);
            session("session", sessionId);
            Database.getInstance().updateSession(name, sessionId);
            return redirect("/");
        } else {
            return TODO;
        }
    }

    public static Result logout() {
        String name = session("name");

        session().clear();
        Database.getInstance().updateSession(name, null);
        return redirect("/");
    }
}
