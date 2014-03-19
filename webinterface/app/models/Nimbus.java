package models;

import com.github.kevinsawicki.http.*;

public class Nimbus {

  private static String ip;

  public static String getIp() {
    String result = Nimbus.ip;
    if(Nimbus.ip == null) {
      String requestUrl = "http://54.195.243.38:9000/lookup?type=nimbus";
      result = HttpRequest.get(requestUrl).body().replaceAll("\\s","");
      Nimbus.ip = result;
    }
    return result;
  }

  public static Boolean isUp() {
    return Nimbus.getIp() != null;
  }
}