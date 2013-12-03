package stormFS.operators;

import java.util.List;

import stormFS.udf.ISerializiableExecutable;

public interface FilterUDF extends
		ISerializiableExecutable<List<Object>, Boolean> {

}
