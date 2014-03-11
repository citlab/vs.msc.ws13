package de.tu_berlin.citlab.storm.topologies;

import de.tu_berlin.citlab.storm.operators.MultipleOperators;
import de.tu_berlin.citlab.storm.operators.OperatorProcessingDescription;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class FilterBadUserMultipleOperators extends MultipleOperators implements Serializable {
    public FilterBadUserMultipleOperators( OperatorProcessingDescription ... operators ){
        super(operators);
    }
    public static final Map<String,Integer> badUsers = new HashMap<String,Integer>();
    public static boolean isDetectedUser(String user){
        if( FilterBadUserMultipleOperators.badUsers.containsKey(user) ){
            return true;
        } else {
            return false;
        }
    }
    public static void updateDetectedUser(String user, int total_significance ){
        if( isDetectedUser(user) ){
            Integer new_significance = FilterBadUserMultipleOperators.badUsers.get(user) + total_significance;
            FilterBadUserMultipleOperators.badUsers.put(user, new_significance);

            System.out.println("new detected user: " + user + ", totalsignificance: " + new_significance );

        } else {
            FilterBadUserMultipleOperators.badUsers.put(user, total_significance);

            System.out.println("new detected user: " + user + ", totalsignificance: " + total_significance);
        }
    }
}
