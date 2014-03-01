package de.tu_berlin.citlab.storm.helpers;

import backtype.storm.Constants;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.operators.StaticTuple;

public class TupleHelper {

	public static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& tuple.getSourceStreamId().equals(
						Constants.SYSTEM_TICK_STREAM_ID);
	}


    public static Tuple createStaticTuple(Fields fields, Values values ){
        return new StaticTuple(fields, values );
    }

}
