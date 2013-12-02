package de.tu_berlin.citlab.storm.bolts;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.bolts.windows.BucketStore;
import de.tu_berlin.citlab.storm.bolts.windows.BucketStore.WinTypes;
import de.tu_berlin.citlab.storm.udf.IBatchOperator;


public class UDFWindowBolt<Key extends Comparable<Key>>	extends BaseRichBolt
{
	private static final long serialVersionUID = 1L;
	
	
/* Global Constants: */
/* ================= */
	
	protected final Fields _inputFields;
	protected final Fields _outputFields;
	protected final BucketStore<Key, Values> _bucketStore;
	
	protected final IBatchOperator<Key> _batchOp;
	
/* Global Variables: */
/* ================= */
	
	private OutputCollector _collector;
	
	
/* Constructor: */
/* ============ */
	
	public UDFWindowBolt(Fields inputFields, Fields outputFields, int winCount, WinTypes winType, IBatchOperator<Key> batchOp) 
	{
		_inputFields = inputFields;
		_outputFields = outputFields;
		_bucketStore = new BucketStore<Key, Values>(winCount, winType);
		
		_batchOp = batchOp;
		
	}
	
	
	
/* Public Methods (from BaseRichBolt): */
/* =================================== */
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) 
	{
		declarer.declare(_outputFields);
	}


	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) 
	{
		_collector = collector;
		
	}
	
	@Override
	/**
	 * Add Tick-Tuples to the Streaming-Bolt in order to call "execute()"
	 * not only when new real Tuples arrive.
	 */
	public Map<String, Object> getComponentConfiguration() 
	{
		Config conf = new Config();
        int tickFreqInSecs = 1;
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFreqInSecs);
        return conf;
	}



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
						_collector.emit(outputValue);
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
