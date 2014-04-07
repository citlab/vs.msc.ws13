package models;

import models.aws.Instance;
import models.aws.AwsCli;

import com.github.kevinsawicki.http.*;

public class Supervisor implements Server {

  public String getIp() {
    String requestUrl = "http://54.195.243.38:9000/lookup?type=supervisor";
    return HttpRequest.get(requestUrl).body().replaceAll("\\s","");
  }

  public Boolean isUp() {
    return !this.getIp().equals("null");
  }

  public String getStatus() {
    try {
      Instance[] instances = AwsCli.describeInstances();
      Instance instance = null;
      String imageId = ClusterDatabase.getInstance().getProperty("supervisor.image-id");

      for(Instance inst : instances) {
        if(inst.getImageId().equals(imageId)) instance = inst;      
      }

      return instance == null ? "Stopped" : instance.toString();
    } catch(Exception e) {
      return "Local";
    }
  }

  public String getInstanceData() {
    return "{blah: test}";
  }
}