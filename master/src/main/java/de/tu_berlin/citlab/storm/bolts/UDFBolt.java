package de.tu_berlin.citlab.storm.bolts;

import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.helpers.TupleHelper;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.storm.window.TimeWindow;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.storm.window.WindowHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class UDFBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger("UDFBolt");


/* Global Variables: */
/* ================= */
	
	protected OutputCollector collector;

/* Global Constants: */
/* ================= */
	
	final protected Fields outputFields;

	final protected IOperator operator;

	final protected WindowHandler windowHandler;

	
	
/* Constructors: */
/* ============= */
	
	public UDFBolt(Fields outputFields, IOperator operator) {
		this(outputFields, operator, new CountWindow<Tuple>(1));
	}

	public UDFBolt(Fields outputFields, IOperator operator,
			Window<Tuple, List<Tuple>> window) {
		this(outputFields, operator, window, KeyConfigFactory.DefaultKey());
	}
	
	public UDFBolt(Fields outputFields, IOperator operator,
			Window<Tuple, List<Tuple>> window, IKeyConfig keyConfig) {
		this.outputFields = outputFields;
		this.operator = operator;
		windowHandler = new WindowHandler(window, keyConfig);
	}
	
	
	
/* Public Methods: */
/* =============== */

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if (outputFields != null) {
			declarer.declare(outputFields);
		}
	}

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		if (windowHandler.getStub() instanceof TimeWindow) {
			conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
					((TimeWindow<Tuple>) windowHandler.getStub()).getTimeSlot());
		}
		return conf;
	}

	
	public void execute(Tuple input) {
		if (TupleHelper.isTickTuple(input)) {
			executeBatches(windowHandler.flush());
		}
		else {
			windowHandler.add(input);
			if (windowHandler.isSatisfied()) {
				executeBatches(windowHandler.flush());
			}
		}

	}

	
	
/* Private Methods: */
/* ================ */
	
	protected void executeBatches(List<List<Tuple>> windows) {
        for (List<Tuple> window : windows) {
                operator.execute(window, collector );

                // if succeed i can always say i processed it
                for (Tuple tuple : window) {
                        collector.ack(tuple);
                }
        }//for
	}

}
