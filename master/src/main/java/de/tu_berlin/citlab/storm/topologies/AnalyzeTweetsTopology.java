package de.tu_berlin.citlab.storm.topologies;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.db.CassandraConfig;
import de.tu_berlin.citlab.db.PrimaryKey;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.helpers.TupleHelper;
import de.tu_berlin.citlab.storm.operators.*;
import de.tu_berlin.citlab.storm.operators.join.StaticHashJoinOperator;
import de.tu_berlin.citlab.storm.operators.join.TupleProjection;
import de.tu_berlin.citlab.storm.spouts.TwitterSpout;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.storm.window.TimeWindow;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.twitter.TwitterConfiguration;
import de.tu_berlin.citlab.twitter.TwitterUserLoader;

import java.io.Serializable;
import java.util.*;

public class AnalyzeTweetsTopology implements Serializable{
    private static final int windowSize = 10;
    private static final int slidingOffset = 10;

    public Window<Tuple, List<Tuple>> COUNT_WINDOW =new CountWindow<Tuple>(windowSize, slidingOffset);
    //public Window<Tuple, List<Tuple>> TIME_WINDOW =new CountWindow<Tuple>(windowSize, slidingOffset);

    public TwitterSpout createTwitterSpout() throws Exception {
        // Setup up Twitter configuration
        Properties user = TwitterUserLoader.loadUser("twitter.config");
        String[] keywords = new String[] {"der", "die", "das", "wir", "ihr", "sie" };
        String[] languages = new String[] {"de"};
        String[] outputFields = new String[] {"user", "id", "tweet"};
        TwitterConfiguration config = new TwitterConfiguration(user, keywords, languages, outputFields);
        return new TwitterSpout(config);
    }

    public UDFBolt createCassandraSink(){

        CassandraConfig cassandraCfg = new CassandraConfig();
        cassandraCfg.setParams(  //optional, but defaults not always sensable
                "127.0.0.1",
                "citstorm",
                "tweets",
                new PrimaryKey("user", "id"), /* CassandraFactory.PrimaryKey(..)  */
                new Fields() /*save all fields ->  CassandraFactory.SAVE_ALL_FIELD  */
        );

        return new UDFBolt(
                new Fields( "user", "id", "tweet" ),
                new CassandraOperator(cassandraCfg),
                COUNT_WINDOW
        );
    }

    public UDFBolt flatMapTweetWords(){
        return new UDFBolt(
                new Fields( "user", "id", "word" ),
                new IOperator(){

                    @Override
                    public void execute(List<Tuple> tuples, OutputCollector collector) {
                        for( Tuple p : tuples ){
                            String[] words = p.getStringByField("tweet").split(" ");
                            for( String word : words ){
                                collector.emit(new Values(p.getValueByField("user"),p.getValueByField("id"), word.trim().toLowerCase() ));
                            }//for
                        }//for
                    }// execute()
                },
                COUNT_WINDOW
                );
    }

    public UDFBolt createStaticHashJoin(){

        IKeyConfig groupKey = new IKeyConfig(){
            public Serializable getKeyOf( Tuple tuple) {
                Serializable key = tuple.getSourceComponent();
                return key;
            }
        };


        TupleProjection projection = new TupleProjection(){
            public Values project(Tuple left, Tuple right) {
                return new Values(
                        right.getValueByField("user"),
                        right.getValueByField("id"),
                        right.getValueByField("word"),
                        left.getValueByField("significance")
                );
            }
        };

        List<Tuple> badWordJoinSide = new ArrayList<Tuple>();
        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("bombe", 100)) );
        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("nuklear", 500)) );
        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("anschlag", 1000)) );
        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("berlin", 10)) );
        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("macht", 100)) );
        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("religion", 200)) );
        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("gott", 50)) );
        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("allah", 1000)) );
        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("heilig", 500)) );

        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("der", 100)) );
        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("die", 100)) );
        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("das", 100)) );

        return new UDFBolt(
                new Fields( "user", "id", "word", "significance" ),
                new StaticHashJoinOperator(
                        KeyConfigFactory.compareByFields(new Fields("word")),
                        projection,
                        badWordJoinSide.iterator() ),
                COUNT_WINDOW
        );
    }

    public UDFBolt reduceUserSignificance(){
        return new UDFBolt(
                new Fields( "user", "total_significance" ),
                new IOperator(){
                    @Override
                    public void execute(List<Tuple> tuples, OutputCollector collector) {
                        String user=tuples.get(0).getStringByField("user");
                        int total_significance=0;
                        for( Tuple p : tuples ){
                            int significance = p.getIntegerByField("significance");
                            total_significance+=significance;
                        }//for

                        collector.emit(new Values(user,total_significance ));

                    }// execute()
                },
                new CountWindow<Tuple>(1, 1), //new TimeWindow<Tuple>(1000, 1000),
                KeyConfigFactory.ByFields("user")
        );

    }

    public UDFBolt filterBadUsers(){

        return new UDFBolt(
                new Fields( "user", "id", "tweet" ), // output
                new MultipleOperators(
                    // process new bad users
                    new OperatorProcessingDescription(
                            new IOperator(){
                                @Override
                                public void execute(List<Tuple> tuples, OutputCollector collector) {
                                    for( Tuple t : tuples ){
                                        String user = t.getStringByField("user");
                                        int totalsignificance = t.getIntegerByField("total_significance");

                                        // process detected user
                                        int sig = BadUserDatabase.updateDetectedUser(user, totalsignificance);

                                        // do not output any tuples
                                    }
                                }// execute()
                            },
                            "reduce_to_user_significance"
                    ),
                    // process raw comping tweets
                    new OperatorProcessingDescription(
                        new FilterOperator(
                                new Fields("user", "id", "tweet"), // input
                                new FilterUDF() {
                                    @Override
                                    public void prepare() {
                                        //TODO: Load from database if data exists
                                    }

                                    @Override
                                    public Boolean evaluate(Tuple tuple ) {
                                        String user = tuple.getStringByField("user");
                                        return BadUserDatabase.isDetectedUser(user);
                                    }
                                }),
                            "tweets"
                        )
                ),
            new CountWindow<Tuple>(1, 1), //new TimeWindow<Tuple>(2000, 2000),
            KeyConfigFactory.BySource()
        );
    }



    public StormTopology createTopology() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        // provide twitter streaminh data
        builder.setSpout("tweets", createTwitterSpout(), 1);

        // filter and find bad users
        builder.setBolt("filter_bad_users", filterBadUsers(), 1)
                .shuffleGrouping("tweets")
                .shuffleGrouping("reduce_to_user_significance");

        //builder.setBolt("store_tweets", createCassandraSink(), 1)
        //        .shuffleGrouping("filter_bad_users");

        // find bad users
        builder.setBolt("flatmap_tweet_words", flatMapTweetWords(), 1)
                .shuffleGrouping("tweets");

        builder.setBolt("join_with_badwords", createStaticHashJoin(), 1)
                .shuffleGrouping("flatmap_tweet_words");

        builder.setBolt("reduce_to_user_significance", reduceUserSignificance(), 1)
                .shuffleGrouping("join_with_badwords");



        return builder.createTopology();
    }


    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        conf.setDebug(true);

        conf.setMaxTaskParallelism(1);
        conf.setMaxSpoutPending(1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("analyzte-twitter-stream", conf,
            new AnalyzeTweetsTopology().createTopology() );
    }
}
