package models;

import models.Database;
import java.util.*;

public class Topology {
  private String title;
  private String user;
  private String date;
  private String name;

  public Topology(String title, String user, String date, String name) {
    this.title = title;
    this.user = user;
    this.date = date;
    this.name = name;
  }

  public static ArrayList<Topology> readFiles(String user) {
    ArrayList<Topology> list = FileDatabase.getInstance().getFilesForUser(user);

    return list;
  }

  public String getTitle() {
    return this.title;
  }

  public String getUser() {
    return this.user;
  }

  public String getDate() {
    return this.date;
  }

  public String getName() {
    return this.name;
  }
}