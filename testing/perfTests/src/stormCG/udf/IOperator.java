package stormCG.udf;

import backtype.storm.tuple.Values;

public interface IOperator extends ISerializiableExecutable<Values, Values[]> {
	
}
