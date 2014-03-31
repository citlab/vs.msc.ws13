package models;

import java.util.*;

public interface Server {
  public String getStatus();
  public Boolean isUp();
  public String getIp();
}