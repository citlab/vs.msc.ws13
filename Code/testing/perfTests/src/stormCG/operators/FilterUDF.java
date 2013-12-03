package stormCG.operators;

import backtype.storm.tuple.Values;
import stormCG.udf.ISerializiableExecutable;

public interface FilterUDF extends ISerializiableExecutable<Values, Boolean> {

}
