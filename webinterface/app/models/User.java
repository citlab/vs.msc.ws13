package models;

import play.*;
import play.mvc.*;
import play.mvc.Http.*;

import models.Database;

public class User {

  private String name;
  private int id;
  private String session;

  public User(String name, int id, String session) {
    this.name = name;
    this.id = id;
    this.session = session;
  }

  public String getName() {
    return this.name;
  }

  public int getId() {
    return this.id;
  }

  public String getStatus() {
    return session != null ? "online" : "offline";
  }

  public static boolean loggedIn(boolean onlyLoggedIn, String name, String sessionId) {
    if (!onlyLoggedIn) return true;
    return name!=null && Database.getInstance().checkSession(name, sessionId);
  }

  public static boolean isAdmin(String name) {
    return name.equals("admin");
  }


}