package de.tu_berlin.citlab.storm.operators.join;

import java.io.Serializable;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TupleProjection implements Serializable {
	public Values project(Tuple outer, Tuple inner ) { return null; };
    public static TupleProjection project(final Fields outer, final Fields inner){
        return
        new TupleProjection() {
            public Values project(Tuple inMemTuple, Tuple tuple) {
                return new Values( tuple.select(inner), inMemTuple.select(outer) );
            }
        };
    }
}
