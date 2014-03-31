package de.tu_berlin.citlab.storm.bolts;

import java.util.List;
import java.util.Map;

import de.tu_berlin.citlab.storm.udf.UDFOutput;
import org.apache.commons.lang.StringUtils;
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
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;


public class UDFBolt extends BaseRichBolt implements UDFOutput {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger("Bolt");
    private static final Marker marker = MarkerManager.getMarker("Topology");
    private static final Marker statistics = MarkerManager.getMarker("Statistics");
    private static final Marker debugger = MarkerManager.getMarker("Debug");
    private static final String LOG_DLIMITER=",";

    // SCOPE describes the location of log messages created (e.g. operator)

    public void log_debug(String msg){ log_debug("UDF", msg); };
    public void log_debug(String scope, Object ... msgs){
        LOGGER.debug(marker, stormComponentId+LOG_DLIMITER+stormTaskId+LOG_DLIMITER+scope+LOG_DLIMITER+ StringUtils.join(msgs, ' '));
    }

    public void log_info(String msg){ log_info("UDF", msg); };
    public void log_info(String scope, Object ... msgs){
        LOGGER.info(marker, stormComponentId+LOG_DLIMITER+stormTaskId+LOG_DLIMITER+scope+LOG_DLIMITER+ StringUtils.join(msgs, ' '));
    }
    public void log_error(String msg){ log_error("UDF", msg); };
    public void log_error(String scope, Object ... msgs){
        LOGGER.error(marker, stormComponentId + LOG_DLIMITER + stormTaskId + LOG_DLIMITER + scope + LOG_DLIMITER + StringUtils.join(msgs, ' '));
    }
    public void log_statistics(String msg){ log_statistics("UDF", msg); };
    public void log_statistics(String scope, Object ... msgs){
        LOGGER.info(statistics, stormComponentId + LOG_DLIMITER + stormTaskId + LOG_DLIMITER+ scope + LOG_DLIMITER + StringUtils.join(msgs, ' '  ));
    }

    
/* Global Variables: */
/* ================= */

    protected OutputCollector collector;

/* Global Constants: */
/* ================= */

    final protected Fields outputFields;
    final protected IOperator operator;
    final protected WindowHandler windowHandler;

    // important for logging
    private String stormComponentId     = "default";
    private int stormTaskId             = -1;



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
        this.operator.setUDFBolt(this);
        windowHandler = new WindowHandler(window, windowKey, groupByKey);
        windowHandler.setUDFBolt(this);
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
        this.stormComponentId = context.getThisComponentId();
        this.stormTaskId = context.getThisTaskIndex();
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


    public String getUDFDescription(){
        return "Bolt<"+stormComponentId+">::UDF<"+operator.getClass().getSimpleName()+"> -> (" +StringUtils.join(outputFields.toList().toArray(), ",")+")";
    }


/* Private Methods: */
/* ================ */

    protected void executeBatches(List<List<Tuple>> windows) {
        for (List<Tuple> window : windows) {
            log_debug("Executing Operator with window {}", LogPrinter.toTupleListString(window));
			try {
				operator.execute(window, collector );

                log_debug("executeBatch:size "+window.size());
				for (Tuple tuple : window) {
                    log_info("executeBatch:tuple:" +tuple );
                    collector.ack(tuple);
				}
			}
			catch (OperatorException e) {
				log_error("Operator Execution resulted in an error!", e);
			}
        }//for
    }

}