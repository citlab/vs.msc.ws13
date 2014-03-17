package de.tu_berlin.citlab.storm.bolts;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import de.tu_berlin.citlab.storm.exceptions.OperatorException;
import de.tu_berlin.citlab.testsuite.helpers.LogPrinter;


public class UDFBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger("Bolt");


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
                   Window<Tuple, List<Tuple>> window, IKeyConfig windowKey) {
        this(outputFields, operator, window, windowKey, KeyConfigFactory.DefaultKey());
    }

    public UDFBolt(Fields outputFields, IOperator operator,
                   Window<Tuple, List<Tuple>> window, IKeyConfig windowKey, IKeyConfig groupByKey) {
        this.outputFields = outputFields;
        this.operator = operator;
        windowHandler = new WindowHandler(window, windowKey, groupByKey);
    }



/* Getters & Setters */
/* ================= */

    public Fields getOutputFields() {
        return outputFields;
    }
    public IOperator getOperator() {
        return operator;
    }
    public WindowHandler getWindowHandler() {
        return windowHandler;
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
            List<List<Tuple>> windows = windowHandler.addSafely(input);
            if (windows != null) {
                executeBatches(windows);
            }
        }

    }



/* Private Methods: */
/* ================ */

    protected void executeBatches(List<List<Tuple>> windows) {
        for (List<Tuple> window : windows) {
			LOGGER.info("Executing Operator with window {}", LogPrinter.toTupleListString(window));
			try {
				operator.execute(window, collector );

				for (Tuple tuple : window) {
					collector.ack(tuple);
				}
			}
			catch (OperatorException e) {
				LOGGER.error("Operator Execution resulted in an error!", e);
			}

        }//for
    }

}