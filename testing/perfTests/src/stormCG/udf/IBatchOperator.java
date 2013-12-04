package stormCG.udf;

import java.util.List;

import backtype.storm.tuple.Values;

public interface IBatchOperator extends IBatchExecutable<List<Object>, List<Values>, List<Values[]>>{

}
