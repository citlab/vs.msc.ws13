package models;

import play.*;
import play.mvc.*;
import play.mvc.Http.*;

import models.Database;

public class User {
  public static boolean loggedIn(boolean onlyLoggedIn, String name, String sessionId) {
    if (!onlyLoggedIn) return true;
    return name!=null && Database.getInstance().checkSession(name, sessionId);
  }
}