package models;

import com.github.kevinsawicki.http.*;

public class Nimbus {

  public static String getIp() {
    String requestUrl = "http://54.195.243.38:9000/lookup?type=nimbus";
    return HttpRequest.get(requestUrl).body().replaceAll("\\s","");
  }

  public static Boolean isUp() {
    return Nimbus.getIp() != null;
  }
}