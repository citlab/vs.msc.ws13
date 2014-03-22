package controllers;

import play.*;
import play.mvc.*;

import views.html.*;
import models.LogEntry;
import models.Database;

import play.libs.Json;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;

public class Log extends Controller {

  @BodyParser.Of(BodyParser.Json.class)
  public static Result getLatest(long lastId) {
    ObjectNode result = Json.newObject();

    //int lastId = Integer.parseInt(request().getQueryString("lastId"));
    ArrayList<LogEntry> list = Database.getInstance().getLogs(lastId);

    for(LogEntry entry : list) {
      ObjectNode message = Json.newObject();

      message.put("id", entry.getId());
      message.put("time", entry.getDatetime());
      message.put("bolt", entry.getLogger());
      message.put("message", entry.getMessage());

      boolean hasError = entry.getException() != null && !entry.getException().isEmpty();
      message.put("hasError", hasError);

      result.put(new Integer(entry.getId()).toString(), message);
    }
    return ok(result);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result getLog(long lastId) {
    ObjectNode result = Json.newObject();
    LogEntry entry = Database.getInstance().getLog(lastId);;

    result.put("logger", entry.getLogger());
    result.put("level", entry.getLevel());
    result.put("exception", entry.getException());
    result.put("message", entry.getMessage());
    result.put("id", entry.getId());
    result.put("time", entry.getDatetime());
    result.put("milliseconds", entry.getMilliseconds());
    result.put("thread", entry.getThread());
    result.put("marker", entry.getMarker());

    return ok(result);
  }

}
