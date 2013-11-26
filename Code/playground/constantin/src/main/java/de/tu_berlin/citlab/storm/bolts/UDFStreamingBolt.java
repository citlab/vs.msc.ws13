package de.tu_berlin.citlab.storm.bolts;

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
import de.tu_berlin.citlab.storm.spouts.FieldKeys;
import de.tu_berlin.citlab.storm.udf.IOperator;

public class UDFStreamingBolt			extends BaseRichBolt 
										implements UDFStreaming<Tuple>
{

	private static final long serialVersionUID = 1L;
	
	
/* Global Variables: */
/* ================= */
	
	protected final Fields _inputFields;
	protected final Fields _outputFields;
	protected final BucketStore<Tuple> _bucketStore;
	
	protected final IOperator _operator;
	private OutputCollector _collector;
	
	
/* Constructor: */
/* ============ */
	
	public UDFStreamingBolt(FieldKeys fieldKeys, Fields inputFields, Fields outputFields, IOperator operator) 
	{
		this._inputFields = inputFields;
		this._outputFields = outputFields;
		this._bucketStore = new BucketStore<Tuple>(fieldKeys, 1000);
		
		this._operator = operator;
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
		this.emissionRequest();
		
		if(this.isTickTuple(input) == false)
		{
			this.groupBy(input);
			
			Values params = (Values) input.select(_inputFields);
			Values[] outputValues = _operator.execute(params);
			if(outputValues != null) {
				for(List<Object> outputValue : outputValues) 
				{
					_collector.emit(outputValue);
				}
			}
		}
		
	}

	public void groupBy(Tuple input) 
	{
		// TODO Auto-generated method stub
		
	}
	
	public void emissionRequest() 
	{
		//_bucketStore. TODO: call a prepare_emission() function.	
	}	

	
/* Private Methods: */
/* ================ */

	private boolean isTickTuple(Tuple tuple) 
	{
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
	            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}
	
}
