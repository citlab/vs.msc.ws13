package de.tu_berlin.citlab.storm.topologies;

import de.tu_berlin.citlab.storm.operators.MultipleOperators;
import de.tu_berlin.citlab.storm.operators.OperatorProcessingDescription;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class BadUserDatabase extends MultipleOperators implements Serializable {

    public BadUserDatabase(OperatorProcessingDescription... operators){
        super(operators);
    }
    public static final Map<String,Integer> badUsers = new HashMap<String,Integer>();
    public static int SIGNIFICANCE_THRESHOLD = 500;

    public static boolean isDetectedUser(String user){
        if( existUser(user) ){
            Integer sig = BadUserDatabase.badUsers.get(user);

           return sig >= SIGNIFICANCE_THRESHOLD;
        } else {
            return false;
        }
    }

    public static boolean existUser(String user){
        if( BadUserDatabase.badUsers.containsKey(user) ){
            return true;
        } else {
            return false;
        }
    }

    synchronized public static int updateDetectedUser(String user, int total_significance ){
        if( isDetectedUser(user) ){
            Integer new_significance = BadUserDatabase.badUsers.get(user) + total_significance;
            BadUserDatabase.badUsers.put(user, new_significance);

            return new_significance;
        } else {
            BadUserDatabase.badUsers.put(user, total_significance);

            return total_significance;
        }
    }
}
