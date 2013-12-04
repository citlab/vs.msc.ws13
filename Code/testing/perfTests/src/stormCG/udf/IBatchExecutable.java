package stormCG.udf;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public interface IBatchExecutable<K, I, O> extends Serializable {
	
	public O execute_batch(HashMap<K, I> entryMap); 
}
