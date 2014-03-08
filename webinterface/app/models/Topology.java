package models;

import java.util.*;

public class Topology {
  private String title;
  private String date;

  public Topology(String title, String date) {
    this.title = title;
    this.date = date;
  }

  public static ArrayList<Topology> readFiles() {
    ArrayList<Topology> list = new ArrayList<Topology>();
    list.add(new Topology("Blah","Heute"));
    list.add(new Topology("Schön","Winter"));
    list.add(new Topology("Doof","Irgendwann"));
    list.add(new Topology("Cool","Gestern"));

    return list;
  }

  public String getTitle() {
    return this.title;
  }

  public String getDate() {
    return this.date;
  }
}