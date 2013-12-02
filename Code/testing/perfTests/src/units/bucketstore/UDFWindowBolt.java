package units.bucketstore;


import java.util.HashMap;
import java.util.List;

import units.bucketstore.BucketStore.WinTypes;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class UDFWindowBolt<Key>
{
	
/* Global Constants: */
/* ================= */
	
	protected final Fields _inputFields;
	protected final BucketStore<Key, Values> _bucketStore;
	
	
/* Constructor: */
/* ============ */
	
	public UDFWindowBolt(Fields inputFields, int winCount, WinTypes winType) 
	{
		_inputFields = inputFields;
		_bucketStore = new BucketStore<Key, Values>(winCount, winType);		
	}
	
	
	
/* Public Methods (from BaseRichBolt): */
/* =================================== */


	public void execute(Key sortKey, Tuple input) 
	{
	//Check for full Windows in each execute-loop and execute those:
		HashMap<Key, List<Values>> readyTupleMap = _bucketStore.readyForExecution();
		if(readyTupleMap.isEmpty() == false)
		{
			List<Values[]> returnVals = executeBatch(readyTupleMap);
		}	
			
	//If the current input tuple is no tick-tuple, then add it by a key to one window:		
		Values params = (Values) input.select(_inputFields); //<- TODO Why problems here?
		_bucketStore.sortInBucket(sortKey, params);		
	}
	
	private List<Values[]> executeBatch(HashMap<Key, List<Values>> tupleMap)
	{
		// TODO Auto-generated method stub
		return null;
	}
	
}
