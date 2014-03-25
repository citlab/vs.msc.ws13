package de.tu_berlin.citlab.storm.topologies;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import clojure.lang.IMeta;
import clojure.lang.Indexed;
import clojure.lang.Seqable;
import de.tu_berlin.citlab.db.CassandraConfig;
import de.tu_berlin.citlab.db.CassandraDAO;
import de.tu_berlin.citlab.db.PrimaryKey;
import de.tu_berlin.citlab.logging.LoggingConfigurator;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.helpers.TupleHelper;
import de.tu_berlin.citlab.storm.operators.*;
import de.tu_berlin.citlab.storm.operators.join.StaticHashJoinOperator;
import de.tu_berlin.citlab.storm.operators.join.TupleProjection;
import de.tu_berlin.citlab.storm.spouts.CassandraDataProviderSpout;
import de.tu_berlin.citlab.storm.spouts.TwitterSpout;
import de.tu_berlin.citlab.storm.spouts.UDFSpout;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.*;
import de.tu_berlin.citlab.twitter.InvalidTwitterConfigurationException;
import de.tu_berlin.citlab.twitter.TwitterConfiguration;
import de.tu_berlin.citlab.twitter.TwitterUserLoader;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;


public class AnalyzeTweetsTopology implements TopologyCreation
{
    private static final int windowSize = 1;
    private static final int slidingOffset = 1;
    //public Window<Tuple, List<Tuple>> WINDOW = new CountWindow<Tuple>(windowSize, slidingOffset);
    public Window<Tuple, List<Tuple>> WINDOW = new TimeWindow<Tuple>(1,1);

    public BaseRichSpout createTwitterSpout() throws InvalidTwitterConfigurationException {
        // Setup up Twitter configuration
        Properties user = TwitterUserLoader.loadUser("twitter.config");
        String[] keywords = new String[] {"der", "die","das","wir","ihr","sie", "dein", "mein", "facebook", "google", "twitter" };
        String[] languages = new String[] {"de"};
        String[] outputFields = new String[] {"user", "tweet_id", "tweet"};
        TwitterConfiguration config = new TwitterConfiguration(user, keywords, languages, outputFields);
        return new TwitterSpout(config);
    }

    public CassandraConfig getCassandraConfig(){
        CassandraConfig cassandraCfg = new CassandraConfig();
        cassandraCfg.setIP("127.0.0.1");
        //cassandraCfg.setIP(CassandraConfig.getCassandraClusterIPFromClusterManager());

        return cassandraCfg;
    }

    public UDFBolt createCassandraTweetsSink(){
        CassandraConfig cassandraCfg = getCassandraConfig();
        cassandraCfg.setParams(  //optional, but defaults not always sensable
                "citstorm",
                "tweets",
                new PrimaryKey("user", "tweet_id"), /* CassandraFactory.PrimaryKey(..)  */
                new Fields(), /*save all fields ->  CassandraFactory.SAVE_ALL_FIELD  */
                false // no counter
        );

        return new UDFBolt(
                new Fields( "user", "tweet_id", "tweet" ),
                new CassandraOperator( cassandraCfg  ),
                WINDOW
        );
    }

    public UDFBolt createCassandraUserSignificanceSink(){
        CassandraConfig cassandraCfg = getCassandraConfig();
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
    public UDFBolt mapToBadWordsWithCounter(){
        return new UDFBolt(
                new Fields( "word", "count" ),
                new MapOperator( new Mapper() {
                    @Override
                    public List<Object> map(Tuple tuple) {
                        return new Values(tuple.getValueByField("word"), new Long(1) );
                    }
                }),
                WINDOW
        );
    }

    public UDFBolt createCountWordStatisticsCassandraSink(){
        CassandraConfig cassandraCfg = getCassandraConfig();
        cassandraCfg.setParams(  //optional, but defaults not always sensable
                "citstorm",
                "badword_occurences",
                new PrimaryKey("word"), /* CassandraFactory.PrimaryKey(..)  */
                new Fields( "count" ), /*save all fields ->  CassandraFactory.SAVE_ALL_FIELD  */
                true // enable counter-mode
        );

        return new UDFBolt(
                new Fields(),
                new CassandraOperator(cassandraCfg),
                WINDOW
        );
    }


    public UDFBolt flatMapTweetWords(){
        return new UDFBolt(
                new Fields( "user", "tweet_id", "word" ),
                new FlatMapOperator( new FlatMapper() {
                    @Override
                    public List<List<Object>> flatMap(Tuple tuple) {
                        String[] words = tuple.getStringByField("tweet").replaceAll("[^a-zA-Z0-9 ]", "").split(" ");
                        List<List<Object>> result = new ArrayList<>();
                        for( String word : words ){
                            result.add(new Values(tuple.getValueByField("user"), tuple.getValueByField("tweet_id"), word.trim().toLowerCase()));
                        }
                        return result;
                    }
                }),
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

        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("google", new Long(1) )) );
        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("microsoft", new Long(1) )) );
        badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("facebook", new Long(1) )) );

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
                new Fields( "user", "tweet_id", "significance" ),
                new ReduceOperator<Long>( new Reducer<Long>(){
                    @Override
                    public Long reduce(Long value, Tuple tuple) {
                        return tuple.getLongByField("significance") + value;
                    }
                }, new Long(0) /*reducer init value */ ) {
                    @Override
                    public Values envelope(List<Tuple> tuples, Long total_significance){
                        String user=tuples.get(0).getStringByField("user");
                        Long tweet_id=tuples.get(0).getLongByField("tweet_id");
                        return new Values(user,tweet_id,total_significance );
                    }
                },
                WINDOW,
                KeyConfigFactory.ByFields( "user" )
        );
    }

    public UDFBolt filterUserSignificanceByThreshold( final int threshold ){
        return new UDFBolt(
                new Fields( "user", "tweet_id", "significance" ),
                new FilterOperator( new Filter(){
                    @Override
                    public Boolean predicate(Tuple t) {
                        if( t.getLongByField("significance") >= threshold ){
                            return true;
                        } else {
                            return false;
                        }
                    }
                }),
                WINDOW
        );
    }


    public UDFBolt delayTuplesBolt(final int sec, final Fields fields){
        return new UDFBolt(
                fields,
                new DelayTuplesOperator(sec),
                new TimeWindow<Tuple>(sec, sec)
        );
    }

    public UDFSpout persistentTupleProvider(){
        Fields fields = new Fields("user", "significance");
        CassandraConfig cassandraCfg = getCassandraConfig();
        cassandraCfg.setParams("citstorm", "user_significance" );
        return new CassandraDataProviderSpout(fields, cassandraCfg );
    }

    public UDFBolt filterBadUsers(){

        final TupleProjection projection = new TupleProjection(){
            public Values project(Tuple inMemTuple, Tuple tuple) {
                return (Values)tuple.getValues();
            }
        };

        final TupleComparator tupleComparator = KeyConfigFactory.compareByFields(new Fields("user"));

        final Map<Serializable, List<Tuple>> badUsersHT = new HashMap<Serializable, List<Tuple> >();

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
                                        getUDFBolt().log_info("operator", "add new user: " + t);
                                        String user = t.getStringByField("user");
                                        Long currSig = t.getLongByField("significance");

                                        // user exists?
                                        if( badUsersHT.containsKey(tupleComparator.getTupleKey(t)) ){
                                            List<Tuple> keyTuples =  badUsersHT.get(tupleComparator.getTupleKey(t));

                                            // lets assume we have one tuple for each detected user
                                            Long lastSig = keyTuples.get(0).getLongByField("significance");
                                            Long totalSig = lastSig+currSig;

                                            Tuple newUserTuple = TupleHelper.createStaticTuple(new Fields("user", "significance"), new Values(user, totalSig) );
                                            keyTuples.clear();
                                            keyTuples.add(newUserTuple);

                                            // do not output any tuples
                                            getUDFBolt().log_info("operator", "update user: " + t + ", sig: " + totalSig);

                                            getUDFBolt().log_statistics("update user sig: " + totalSig + " " + newUserTuple);

                                        } else {
                                            Long totalSig = currSig;
                                            Tuple newUserTuple = TupleHelper.createStaticTuple(new Fields("user", "significance"), new Values(user, totalSig) );

                                            List<Tuple> badUsers = new ArrayList<Tuple>();
                                            badUsers.add(newUserTuple);

                                            badUsersHT.put(tupleComparator.getTupleKey(t), badUsers );

                                            getUDFBolt().log_statistics("add new user sig: " + totalSig + " " + newUserTuple);

                                            getUDFBolt().log_info("operator","add new user: "+t+", sig: "+totalSig);
                                        }
                                    }
                                }// execute()
                            },
                            "filter_significant_user", "persistent_tuple_provider"
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



    @Override
    public StormTopology createTopology()
    {
        TopologyBuilder builder = new TopologyBuilder();

        // provide twitter streaming data
        try {
            builder.setSpout("tweets", createTwitterSpout(), 1);

        } catch (InvalidTwitterConfigurationException e) {
            e.printStackTrace();
        }

        // find bad users
        builder.setSpout("persistent_tuple_provider", persistentTupleProvider(), 1);

        // find bad users
        builder.setBolt("flatmap_tweet_words", flatMapTweetWords(), 1)
                .shuffleGrouping("tweets");

        builder.setBolt("delayed_tweets", delayTuplesBolt(5, new Fields("user", "tweet_id", "tweet") ), 1 )
                .shuffleGrouping("tweets");

        Fields fieldsGroupByUser = new Fields("user");

        // filter and find bad users
        builder.setBolt("filter_bad_users", filterBadUsers(), 1)
                .fieldsGrouping("filter_significant_user", fieldsGroupByUser)
                .fieldsGrouping("delayed_tweets", fieldsGroupByUser)
                .fieldsGrouping("persistent_tuple_provider", fieldsGroupByUser );

        builder.setBolt("store_tweets", createCassandraTweetsSink(), 1)
                .shuffleGrouping("filter_bad_users");

        builder.setBolt("join_with_badwords", createStaticHashJoin(), 1)
                .shuffleGrouping("flatmap_tweet_words");

        // store bad words into database
        builder.setBolt("badwords_with_counter", this.mapToBadWordsWithCounter(), 1)
                .shuffleGrouping("join_with_badwords");

        builder.setBolt("badwords_with_counter_sink", createCountWordStatisticsCassandraSink(), 1)
                .shuffleGrouping("badwords_with_counter");

        builder.setBolt("reduce_to_user_significance", reduceUserSignificance(), 1)
                .fieldsGrouping("join_with_badwords", fieldsGroupByUser);

        // filter only user with a specific significance
        builder.setBolt("filter_significant_user", filterUserSignificanceByThreshold(ConspicuousUserDatabase.SIGNIFICANCE_THRESHOLD), 1)
                .shuffleGrouping("reduce_to_user_significance");

        builder.setBolt("store_user_significance", createCassandraUserSignificanceSink(), 1)
                .shuffleGrouping("filter_significant_user");

        return builder.createTopology();

    }
}
