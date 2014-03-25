package de.tu_berlin.citlab.storm.operators.join;

import java.io.Serializable;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.ArrayUtils;

public class TupleProjection implements Serializable {
	public Values project(Tuple outer, Tuple inner ) { return null; };
    public static TupleProjection project(final Fields outer, final Fields inner){
        return
        new TupleProjection() {
            public Values project(Tuple inMemTuple, Tuple tuple) {
                return new Values( ArrayUtils.addAll(tuple.select(outer).toArray(), inMemTuple.select(inner).toArray() ) );
            }
        };
    }
}
