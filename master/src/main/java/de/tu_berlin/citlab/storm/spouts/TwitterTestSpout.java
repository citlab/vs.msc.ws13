package de.tu_berlin.citlab.storm.spouts;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import de.tu_berlin.citlab.twitter.ConfiguredTwitterStreamBuilder;
import de.tu_berlin.citlab.twitter.InvalidTwitterConfigurationException;
import de.tu_berlin.citlab.twitter.TwitterConfiguration;

public class TwitterTestSpout extends BaseRichSpout {

    private static final long serialVersionUID = 8650869752517101545L;
    private final String[] outputFields = {"user", "tweet_id", "tweet"};
    private SpoutOutputCollector collector;
    private boolean emit=false;
    public TwitterTestSpout() {
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(1000);
        if(!emit){
            collector.emit( createOutputValues() );
        } else {
            //collector.emit( new Values("michi", new Integer(2), "keine nachricht") );
            //collector.emit( new Values("michi", new Integer(2), "keine nachricht") );
        }
        emit=true;
    }

    private Values createOutputValues() {
        Object[] values = new Object[outputFields.length];

        values[0] = "kay";
        values[1] = new Integer(1);
        values[2] = "wow google hat mal wieder vollen bullshit erzählt";
        return new Values(values);
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
        declarer.declare(new Fields(outputFields));
    }
}