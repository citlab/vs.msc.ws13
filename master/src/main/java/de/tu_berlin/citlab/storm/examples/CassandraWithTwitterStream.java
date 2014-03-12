package de.tu_berlin.citlab.storm.examples;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

import de.tu_berlin.citlab.db.CassandraConfig;
import de.tu_berlin.citlab.db.CassandraDAO;
import de.tu_berlin.citlab.db.DAO;
import de.tu_berlin.citlab.db.DAOFactory;
import de.tu_berlin.citlab.db.DBConfig;
import de.tu_berlin.citlab.db.PrimaryKey;
import de.tu_berlin.citlab.db.TupleAnalyzer;
import de.tu_berlin.citlab.db.TupleFields;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.operators.CassandraOperator;
import de.tu_berlin.citlab.storm.spouts.TwitterSpout;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.twitter.TwitterConfiguration;
import de.tu_berlin.citlab.twitter.TwitterUserLoader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class CassandraWithTwitterStream {
    private static final int windowSize = 1000;
    private static final int slidingOffset = 500;


    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {
        // prepare storage

        STORAGE.badWords.put("bombe", new BadWord("bombe", 100));
        STORAGE.badWords.put("nuklear", new BadWord("nuklear", 1000));
        STORAGE.badWords.put("anschlag", new BadWord("anschlag", 200));
        STORAGE.badWords.put("religion", new BadWord("religion", 100));
        STORAGE.badWords.put("macht", new BadWord("macht", 300));
        STORAGE.badWords.put("kampf", new BadWord("kampf", 300));

        // Setup up Twitter configuration
        Properties user = TwitterUserLoader.loadUser("twitter.config");
        String[] keywords = new String[] {"der", "die", "das"};
        String[] languages = new String[] {"de"};
        // String[] languages = new String[] { "en", "de" };
        String[] outputFields = new String[] {"user", "id"};
        //String[] outputFields = new String[] {"user", "id"};  // User name as string, Tweet-ID as long
        TwitterConfiguration config = new TwitterConfiguration(user, keywords,
                languages, outputFields);

        Window<Tuple, List<Tuple>> WINDOW_TYPE =new CountWindow<Tuple>(windowSize, slidingOffset);
        //new TimeWindow<Tuple>(windowSize, slidingOffset);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweets", new TwitterSpout(config), 1);

        CassandraConfig cassandraCfg = new CassandraConfig();



        cassandraCfg.setParams(  //optional, but defaults not always sensable
                "myks",
                "mytable1",
                new PrimaryKey("user", "id"), /* CassandraFactory.PrimaryKey(..)  */
                new Fields() /*save all fields ->  CassandraFactory.SAVE_ALL_FIELD  */
        );


        builder.setBolt("save_tweets",
                new UDFBolt(
                        new Fields( "user", "id" ),    //Output Fields momentan nicht verwendet
                        new CassandraOperator(cassandraCfg)
                )).shuffleGrouping("tweets");


        Config conf = new Config();
        conf.setDebug(true);

        conf.setMaxTaskParallelism(1);
        conf.setMaxSpoutPending(1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("sliding-count-window-group-by-test", conf,
                builder.createTopology());
    }
}