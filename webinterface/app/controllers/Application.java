package controllers;

import play.*;
import play.mvc.*;
import play.mvc.Http.*;
import play.mvc.Http.MultipartFormData.*;

import views.html.*;

import java.io.*;
import java.util.*;
import models.*;

public class Application extends Controller {

    public static Result index() {
        return ok(index.render());
    }

    public static Result login() {
        return ok();
    }

    public static Result server() {
        return ok(server.render());
    }

    public static Result kontakt() {
        return ok(kontakt.render());
    }

    public static Result impressum() {
        return ok(impressum.render());
    }

    public static Result deploy() {
        ArrayList<Topology> topologies = Topology.readFiles();
        return ok(deploy.render(topologies));
    }

    public static Result upload() {
        MultipartFormData body = request().body().asMultipartFormData();
        FilePart uploaded = body.getFile("file");
        if (uploaded != null) {
            String fileName = uploaded.getFilename();
            String contentType = uploaded.getContentType(); 
            File file = uploaded.getFile();
            
            flash("notice", "File uploaded");
            return redirect(routes.Application.index());
        } else {
            flash("notice", "Missing file");
            return redirect(routes.Application.deploy());
        }
    }
}
