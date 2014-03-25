package de.tu_berlin.citlab.storm.operators.join;

import java.io.Serializable;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.ArrayUtils;

public class TupleProjection implements Serializable {
	public Values project(Tuple outer, Tuple inner ) { return null; };

    public static TupleProjection project(final Fields tupleFields, final Fields memFields){
        return
        new TupleProjection() {
            public Values project(Tuple tuple, Tuple inMemTuple) {
                System.out.println("mem:"+inMemTuple+", fields:"+memFields);
                System.out.println("tuple:"+tuple+", tuple:"+tupleFields);
                return new Values( ArrayUtils.addAll( tuple.select(tupleFields).toArray(), inMemTuple.select(memFields).toArray()) );
            }
        };
    }
}
