package de.tu_berlin.citlab.storm.examples;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.helpers.TupleHelper;
import de.tu_berlin.citlab.storm.operators.join.*;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.IKeyConfig;

class DataSource2 extends BaseRichSpout {

    private static final long serialVersionUID = -7374814904789368773L;

    private String[] ids = new String[] { "key1", "key2" , "key3" };

    private int currentId = 0;

    int _id = 0;

    SpoutOutputCollector _collector;

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    public void open(@SuppressWarnings("rawtypes") Map conf,
                     TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    public void nextTuple() {
        Utils.sleep(100);
        _collector.emit(new Values(ids[currentId++ % ids.length], _id));
        _id++;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "value"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

public class StaticJoinExample {
    private static final int windowSize = 100;
    private static final int slidingOffset = 100;

    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("s1", new DataSource2(), 1);



        IKeyConfig groupKey = new IKeyConfig(){
            public Serializable getKeyOf( Tuple tuple) {
                Serializable key = tuple.getSourceComponent();
                return key;
            }
        };


        TupleProjection projection = new TupleProjection(){
            public Values project(Tuple left, Tuple right) {
                return new Values(
                        right.getValueByField("key"),
                        right.getValueByField("value"),
                        "blub"
                );
            }
        };

        List<Tuple> staticJoinSide = new ArrayList<Tuple>();

        staticJoinSide.add(TupleHelper.createStaticTuple(new Fields("key"), new Values("key1") ));


        builder.setBolt("slide",
                new UDFBolt(
                        null, // no outputFields
                        new StaticHashJoinOperator( KeyConfigFactory.compareByFields(new Fields("key")),
                                                    projection,
                                                    staticJoinSide.iterator() ),
                        new CountWindow<Tuple>(windowSize, slidingOffset),
                        groupKey
                ),
                1)
                .shuffleGrouping("s1");


        Config conf = new Config();
        conf.setDebug(true);

        conf.setMaxTaskParallelism(1);
        conf.setMaxSpoutPending(1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("sliding-count-window-group-by-test", conf,
                builder.createTopology());
    }
}
