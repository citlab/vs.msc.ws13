package de.tu_berlin.citlab.storm.topologies;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
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
import de.tu_berlin.citlab.storm.spouts.TwitterTestSpout;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.*;
import de.tu_berlin.citlab.twitter.TwitterConfiguration;
import de.tu_berlin.citlab.twitter.TwitterUserLoader;

import java.io.Serializable;
import java.util.*;

public class AnalyzeTweetsTopology implements Serializable{
    private static final int windowSize = 1;
    private static final int slidingOffset = 1;

    //public Window<Tuple, List<Tuple>> WINDOW = new CountWindow<Tuple>(windowSize, slidingOffset);

    public Window<Tuple, List<Tuple>> WINDOW = new TimeWindow<Tuple>(1,1);

    public BaseRichSpout createTwitterSpout() throws Exception {
        // Setup up Twitter configuration
        Properties user = TwitterUserLoader.loadUser("twitter.config");
        String[] keywords = new String[] {"der", "die","das","wir","ihr","sie", "dein", "mein", "facebook", "google", "twitter" };
        String[] languages = new String[] {"de"};
        String[] outputFields = new String[] {"user", "tweet_id", "tweet"};
        TwitterConfiguration config = new TwitterConfiguration(user, keywords, languages, outputFields);
        return new TwitterSpout(config);
    }

    public UDFBolt createCassandraTweetsSink(){
        CassandraConfig cassandraCfg = new CassandraConfig();
        cassandraCfg.setIP( "127.0.0.1" );
        cassandraCfg.setParams(  //optional, but defaults not always sensable
                "citstorm",
                "tweets",
                new PrimaryKey("user", "tweet_id"), /* CassandraFactory.PrimaryKey(..)  */
                new Fields() /*save all fields ->  CassandraFactory.SAVE_ALL_FIELD  */
        );

        return new UDFBolt(
                new Fields( "user", "tweet_id", "tweet" ),
                new CassandraOperator(cassandraCfg),
                WINDOW
        );
    }

    public UDFBolt createCassandraUserSignificanceSink(){
        CassandraConfig cassandraCfg = new CassandraConfig();
        cassandraCfg.setIP( "127.0.0.1" );
        cassandraCfg.setParams(  //optional, but defaults not always sensable
                "citstorm",
                "user_significance",
                new PrimaryKey("user"), /* CassandraFactory.PrimaryKey(..)  */
                new Fields( "significance" ), /*save all fields ->  CassandraFactory.SAVE_ALL_FIELD  */
                true // enable counter-mode
        );

        return new UDFBolt(
                new Fields( "user", "user_significance"),
                new CassandraOperator(cassandraCfg),
                WINDOW
        );
    }


    public UDFBolt flatMapTweetWords(){
        return new UDFBolt(
                new Fields( "user", "tweet_id", "word" ),
                new IOperator(){
                    @Override
                    public void execute(List<Tuple> tuples, OutputCollector collector) {

                        for( Tuple p : tuples ){
                            String[] words = p.getStringByField("tweet").split(" ");
                            for( String word : words ){
                                collector.emit(new Values(p.getValueByField("user"),p.getValueByField("tweet_id"), word.trim().toLowerCase() ));
                            }//for
                        }//for
                    }// execute()
                },
                WINDOW
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
            public Values project(Tuple inMemTuple, Tuple tuple) {
                return new Values(
                        tuple.getValueByField("user"),
                        tuple.getValueByField("tweet_id"),
                        tuple.getValueByField("word"),

                        inMemTuple.getValueByField("significance")
                );
            }
        };

        ConspicuousUserDatabase.SIGNIFICANCE_THRESHOLD = 1;

        List<Tuple> badWordJoinSide = new ArrayList<Tuple>();

        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("google", 1)) );
        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("microsoft", 1)) );
        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("facebook", 1)) );

        /*badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("bombe", 100)) );
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
        */

        return new UDFBolt(
                new Fields( "user", "tweet_id", "word", "significance" ),
                new StaticHashJoinOperator(
                        KeyConfigFactory.compareByFields( new Fields("word")),
                        projection,
                        badWordJoinSide.iterator() ),
                WINDOW
        );
    }

    public UDFBolt reduceUserSignificance(){
        return new UDFBolt(
                new Fields( "user", "significance" ),
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
                WINDOW, //new TimeWindow<Tuple>(1, 1),
                KeyConfigFactory.ByFields("user")
        );
    }

    public UDFBolt delayTuplesBolt(int sec, int slide, Fields fields){
        return new UDFBolt(
                fields,
                new IOperator(){
                    @Override
                    public void execute(List<Tuple> tuples, OutputCollector collector) {
                        for( Tuple p : tuples ){
                            collector.emit(p.getValues());
                        }//for
                    }//execute()
                },
                new TimeWindow<Tuple>(sec, slide)
        );
    }


    public UDFBolt filterBadUsers(){

        final Map<Serializable, List<Tuple>> badUsersHT = new HashMap<Serializable, List<Tuple> >();

        final TupleProjection projection = new TupleProjection(){
            public Values project(Tuple inMemTuple, Tuple tuple) {
                return (Values)tuple.getValues();
           }
        };

        final TupleComparator tupleComparator = KeyConfigFactory.compareByFields(new Fields("user"));

        final StaticHashJoinOperator staticHashJoinBadUsers =
            new StaticHashJoinOperator(
                    tupleComparator,
                    projection,
                    badUsersHT );


        return new UDFBolt(
                new Fields( "user", "tweet_id", "tweet" ), // output
                new MultipleOperators(
                    // process new bad users
                    new OperatorProcessingDescription(
                            new IOperator(){
                                @Override
                                public void execute(List<Tuple> tuples, OutputCollector collector) {
                                    for( Tuple t : tuples ){
                                        System.out.println("add new user: "+t);
                                        String user = t.getStringByField("user");


                                        int currSig = t.getIntegerByField("significance");

                                        // user exists?
                                        if( badUsersHT.containsKey(tupleComparator.getTupleKey(t)) ){
                                            List<Tuple> keyTuples =  badUsersHT.get(tupleComparator.getTupleKey(t));

                                            // lets assume we have one tuple for each detected user
                                            int lastSig = keyTuples.get(0).getIntegerByField("significance");
                                            int totalSig = lastSig+currSig;

                                            Tuple newUserTuple = TupleHelper.createStaticTuple(new Fields("user", "significance"), new Values(user, totalSig) );
                                            keyTuples.clear();
                                            keyTuples.add(newUserTuple);

                                            // do not output any tuples
                                            System.out.println("update user: "+t+", sig: "+totalSig);

                                        } else {
                                            int totalSig = currSig;
                                            Tuple newUserTuple = TupleHelper.createStaticTuple(new Fields("user", "significance"), new Values(user, totalSig) );

                                            List<Tuple> badUsers = new ArrayList<Tuple>();
                                            badUsers.add(newUserTuple);

                                            badUsersHT.put(tupleComparator.getTupleKey(t), badUsers );

                                            System.out.println("add user: "+t+", sig: "+totalSig);
                                        }
                                    }
                                }// execute()
                            },
                            "reduce_to_user_significance"
                    ),
                    // process raw comping tweets
                    new OperatorProcessingDescription(
                            staticHashJoinBadUsers,
                            "delayed_tweets"
                        )
                ),
            WINDOW,
            KeyConfigFactory.BySource()
        );
    }



    public StormTopology createTopology() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        // provide twitter streaminh data
        builder.setSpout("tweets", createTwitterSpout(), 1);

        // find bad users
        builder.setBolt("flatmap_tweet_words", flatMapTweetWords(), 1)
                .shuffleGrouping("tweets");

        builder.setBolt("delayed_tweets", delayTuplesBolt(5, 5, new Fields("user", "tweet_id", "tweet") ), 1 )
                .shuffleGrouping("tweets");

        // filter and find bad users
        builder.setBolt("filter_bad_users", filterBadUsers(), 1)
                .shuffleGrouping("reduce_to_user_significance")
                .shuffleGrouping("delayed_tweets");

        builder.setBolt("store_tweets", createCassandraTweetsSink(), 1)
                .shuffleGrouping("filter_bad_users");


        builder.setBolt("join_with_badwords", createStaticHashJoin(), 1)
                .shuffleGrouping("flatmap_tweet_words");

        builder.setBolt("reduce_to_user_significance", reduceUserSignificance(), 1)
                .shuffleGrouping("join_with_badwords");


        builder.setBolt("store_user_significance", createCassandraUserSignificanceSink(), 1)
                .shuffleGrouping("reduce_to_user_significance");


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
