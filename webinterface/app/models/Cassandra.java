package models;

import com.github.kevinsawicki.http.*;

public class Cassandra {

  public static String getIp() {
    String requestUrl = "http://54.195.243.38:9000/lookup?type=cassandra";
    return HttpRequest.get(requestUrl).body().replaceAll("\\s","");
  }

  public static Boolean isUp() {
    return Cassandra.getIp() != null;
  }
}