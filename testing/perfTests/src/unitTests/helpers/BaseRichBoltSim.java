package unitTests.helpers;

import java.util.Map;

import unitTests.mocks.MockOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


public abstract class BaseRichBoltSim extends BaseRichBolt
{	
	private static final long	serialVersionUID	= 1L;

		public BaseRichBoltSim(OutputCollector mockOColl)
		{
			mockOColl = MockOutputCollector.mockOutputCollector();
		}
		
		public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
				OutputCollector collector)
		{
		}

		abstract public void execute(Tuple input);
		abstract public void declareOutputFields(OutputFieldsDeclarer declarer);

}
