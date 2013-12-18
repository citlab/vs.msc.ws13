package de.tu_berlin.citlab.testsuite.mocks;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


public abstract class BaseRichBoltMock extends BaseRichBolt
{	
	private static final long	serialVersionUID	= 1L;
	
	
/* Global Variables: */
/* ================= */
	
	protected OutputCollector collector;
	
	
/* Public Methods: */
/* =============== */
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector)
	{
		collector = MockOutputCollector.mockOutputCollector();
	}

	abstract public void execute(Tuple input);
	abstract public void declareOutputFields(OutputFieldsDeclarer declarer);

}
