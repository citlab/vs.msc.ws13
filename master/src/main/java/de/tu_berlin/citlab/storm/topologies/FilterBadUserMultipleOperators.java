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
}
