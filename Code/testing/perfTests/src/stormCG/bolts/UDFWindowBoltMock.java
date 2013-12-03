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


public class UDFWindowBoltMock<Key>
{
/* Global Constants: */
/* ================= */
	
	protected final Fields _inputFields;
	protected final Fields _outputFields;
	protected final BucketStore<Key, Values> _bucketStore;
	
	protected final IBatchOperator<Key> _batchOp;	
	
/* Constructor: */
/* ============ */
	
	public UDFWindowBoltMock(Fields inputFields, Fields outputFields, int winCount, WinTypes winType, IBatchOperator<Key> batchOp) 
	{
		_inputFields = inputFields;
		_outputFields = outputFields;
		_bucketStore = new BucketStore<Key, Values>(winCount, winType);
		
		_batchOp = batchOp;
		
	}
	
	
	
/* Public Methods (from BaseRichBolt): */
/* =================================== */


	public void execute(Tuple input) 
	{
	//Check for full Windows in each execute-loop and execute those:
		HashMap<Key, List<Values>> readyTupleMap = _bucketStore.readyForExecution();
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
			Values params = (Values) input.select(_inputFields); //<- TODO Why problems here?
			Key sortKey = _batchOp.sortBy_winKey(params);
			_bucketStore.sortInBucket(sortKey, params);
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
