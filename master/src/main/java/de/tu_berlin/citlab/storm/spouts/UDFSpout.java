package de.tu_berlin.citlab.storm.spouts;


import java.util.Map;

import de.tu_berlin.citlab.storm.udf.UDFOutput;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

abstract public class UDFSpout extends BaseRichSpout implements UDFOutput {

    protected static final Logger LOGGER = LogManager.getLogger("Spout");

    protected SpoutOutputCollector collector;

    protected Fields outputFields;

    public UDFSpout(Fields outputFields) {
        this.outputFields = outputFields;
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        open();
    }

    public SpoutOutputCollector getOutputCollector(){ return this.collector; }

    public void open(){
    }

    @Override
    public void nextTuple() {
    }


    @Override
    public void close() {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(outputFields);
    }
}