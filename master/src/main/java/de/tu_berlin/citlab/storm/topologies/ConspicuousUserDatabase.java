package de.tu_berlin.citlab.storm.topologies;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ConspicuousUserDatabase implements Serializable {

    public ConspicuousUserDatabase(){
    }
    public static final Map<String,Integer> badUsers = new HashMap<String,Integer>();
    public static int SIGNIFICANCE_THRESHOLD = 500;

    public static boolean isDetectedUser(String user){
        if( existUser(user) ){
            Integer sig = ConspicuousUserDatabase.badUsers.get(user);
           return sig >= SIGNIFICANCE_THRESHOLD;
        } else {
            return false;
        }
    }

    public static boolean existUser(String user){
        if( ConspicuousUserDatabase.badUsers.containsKey(user) ){
            return true;
        } else {
            return false;
        }
    }

    synchronized public static int updateDetectedUser(String user, int total_significance ){
        if( ConspicuousUserDatabase.badUsers.containsKey(user) ){
            Integer new_significance = ConspicuousUserDatabase.badUsers.get(user) + total_significance;
            ConspicuousUserDatabase.badUsers.put(user, new_significance);

            return new_significance;
        } else {
            ConspicuousUserDatabase.badUsers.put(user, total_significance);

            return total_significance;
        }
    }
}
