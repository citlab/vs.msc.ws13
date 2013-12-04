package stormCG.bolts;


import java.util.HashMap;
import java.util.List;

import backtype.storm.Constants;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import stormCG.bolts.windows.BucketStore;
import stormCG.bolts.windows.BucketStore.WinTypes;
import stormCG.udf.IBatchOperator;


public class UDFWindowBoltMock
{
/* Global Constants: */
/* ================= */
	
	protected final Fields _inputFields;
	protected final Fields _outputFields;
	protected final Fields _keyFields;
	protected final BucketStore<List<Object>, Values> _bucketStore;
	
	protected final IBatchOperator _batchOp;	
	
	
/* Getters & Setters: */
/* ================== */
	
	public final BucketStore<List<Object>, Values> get_bucketStore() { return _bucketStore; }
	public final IBatchOperator get_batchOp() { return _batchOp; }
	 
	
/* Constructor: */
/* ============ */
	
	public UDFWindowBoltMock(Fields inputFields, Fields outputFields, Fields keyFields, int winCount, WinTypes winType, IBatchOperator batchOp) 
	{
		_inputFields = inputFields;
		_outputFields = outputFields;
		_keyFields = keyFields;
		_bucketStore = new BucketStore<List<Object>, Values>(winCount, winType);
		
		_batchOp = batchOp;
		
	}
	
	
	
/* Public Methods (from BaseRichBolt): */
/* =================================== */


	public void execute(Tuple input) 
	{
	//Check for full Windows in each execute-loop and execute those:
		HashMap<List<Object>, List<Values>> readyTupleMap = _bucketStore.flushBuckets();
		if(readyTupleMap.isEmpty() == false)
		{
			List<Values[]> returnVals = _batchOp.execute_batch(readyTupleMap);
			if(returnVals != null){
				for(Values[] actValArr : returnVals){
					for(List<Object> outputValue : actValArr){
						//_collector.emit(outputValue);
					}
				}
			}
		}	
			
	//If the current input tuple is no tick-tuple, then add it by a key to one window:
		if(this.isTickTuple(input) == false)
		{			
			Values vals = new Values();
			vals.addAll(input.select(_inputFields));
			List<Object> key = input.select(_keyFields); //<- TODO Why problems here?
			_bucketStore.sortInBucket(key, vals);
			//Values[] outputValues = _operator.execute(params);
//			if(outputValues != null) {
//				for(List<Object> outputValue : outputValues) 
//				{
//					_collector.emit(outputValue);
//				}
//			}
		}
		
	}

	
/* Private Methods: */
/* ================ */

	private boolean isTickTuple(Tuple tuple) 
	{
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
	            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}
	
}
