package de.tu_berlin.citlab.storm.operators.join;

import backtype.storm.tuple.Tuple;

public class JoinFactory {
	
	@SuppressWarnings("serial")
	public static JoinPredicate joinByField(final String key ){
		return  new JoinPredicate() {
					public boolean evaluate(Tuple t1, Tuple t2) {
						return t1.getValueByField(key) == t2.getValueByField( key);  
					}
				};
	}

}
