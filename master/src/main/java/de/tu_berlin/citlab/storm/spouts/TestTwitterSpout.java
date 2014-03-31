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


public class TestTwitterSpout  extends BaseRichSpout {

    public static int NextTweetId = 0;

    private static final long serialVersionUID = 8650869752517101545L;
    private final String[] outputFields = {"user", "tweet_id", "tweet"};

    private final String[] users = {"andi", "michi"};

    private final String[] messages = { "keine nachricht",
                                        "wow google hat mal wieder vollen bullshit erzählt",
                                        "Menschen macht gott stark."};

    private SpoutOutputCollector collector;
    private boolean emit=false;

    public TestTwitterSpout() {
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(1000);
        collector.emit( createOutputValues() );
    }

    private Values createOutputValues() {

        NextTweetId++;

        Object[] values = new Object[outputFields.length];

        int user = new java.util.Random().nextInt() % users.length;
        int tweet = new java.util.Random().nextInt() % outputFields.length;

        values[0] = users[user];
        values[1] = NextTweetId;
        values[2] = messages[tweet];
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