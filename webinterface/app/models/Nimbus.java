package models;

import models.aws.Instance;
import models.aws.AwsCli;

import com.github.kevinsawicki.http.*;

public class Nimbus {

  public static String getIp() {
    String requestUrl = "http://54.195.243.38:9000/lookup?type=nimbus";
    return HttpRequest.get(requestUrl).body().replaceAll("\\s","");
  }

  public static Boolean isUp() {
    return Nimbus.getIp() != null;
  }

  public static String getStatus() {
    try {
      Instance[] instances = AwsCli.describeInstances();
      Instance instance = null;
      String imageId = ClusterDatabase.getInstance().getProperty("nimbus.image-id");

      for(Instance inst : instances) {
        if(inst.getImageId().equals(imageId)) instance = inst;      
      }

      return instance == null ? "Stopped" : instance.toString();
    } catch(Exception e) {
      return "Local";
    }
  }
}