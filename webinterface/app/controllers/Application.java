package controllers;

import play.*;
import play.mvc.*;
import play.mvc.Http.*;
import play.mvc.Http.MultipartFormData.*;
import views.html.*;

import java.io.*;
import java.util.*;
import models.*;

import com.github.kevinsawicki.http.*;

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
        ArrayList<Topology> topologies = Topology.readFiles(session("name"));
        return ok(deploy.render(topologies));
    }

    public static Result upload() {
        MultipartFormData body = request().body().asMultipartFormData();
        FilePart uploaded = body.getFile("file");
        if (uploaded != null) {
            String fileName = uploaded.getFilename();
            String contentType = uploaded.getContentType(); 
            File file = uploaded.getFile();
            
            String nimbusIp = new Nimbus().getIp();
            String msg = sendFile(file, nimbusIp, fileName);

            FileDatabase.getInstance().addFile(msg, fileName, session("name"));

            flash("notice", "File uploaded");
            return ok(String.format("{ip: %s, msg: %s}", nimbusIp, msg));
        } else {
            flash("notice", "Missing file");
            return redirect(routes.Application.deploy());
        }
    }

    private static String sendFile(File file, String nimbusIp, String fileName) {
        HttpRequest request = HttpRequest.post("http://"+ nimbusIp +":8081/upload");
        request.part("file", fileName , file);
        return request.ok() ? request.body().replaceAll("\\s","") : request.server();
    }

    public static Result deleteFile(String title) {
        String request = HttpRequest.get("http://"+ new Nimbus().getIp() +":8081/delete", true, "file", title).body();
        String s = FileDatabase.getInstance().deleteFile(title);
        return redirect(routes.Application.deploy());
    }
}
