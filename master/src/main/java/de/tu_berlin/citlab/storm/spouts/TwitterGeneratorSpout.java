package de.tu_berlin.citlab.storm.spouts;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

public class TwitterGeneratorSpout extends BaseRichSpout
{

/* Global Constants: */
/* ================= */
    private static final long serialVersionUID = 8565086975251710154L;
    private static final Logger LOGGER = LogManager.getLogger("Spout");

    private final String[] outputFields = {"user", "tweet_id", "tweet"};

    private final String[] users;
    private final Map<String, Integer> userIDs;
    private final String[] dictionary;
    private final int wordsPerTweet;

    private final int tweetsPerSecond;


/* Global Variables: */
/* ================= */

    private SpoutOutputCollector collector;


/* Constructor: */
/* ============= */

    public TwitterGeneratorSpout(String[] users, String[] dictionary, int wordsPerTweet,
                                 int tweetsPerSecond)
    {
        this.users = users;
        this.dictionary = dictionary;
        this.wordsPerTweet = wordsPerTweet;
        this.userIDs = new HashMap<>(users.length);
        initUserIDs();

        this.tweetsPerSecond = tweetsPerSecond;
    }



/* Public Methods: */
/* =============== */

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(1000/tweetsPerSecond);

        Values tweetVal = generateTweet();
        collector.emit(tweetVal);
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



/* Private Methods: */
/* =============== */

    private void initUserIDs() {
        for (String actUser : users) {
            int randID = (int) Math.round(Math.random() * 10000);
            this.userIDs.put(actUser, randID);
        }
    }

    private Values generateTweet()
    {
        int randUserIndex = (int) Math.round(Math.random() * (users.length -1));
        String randUser = users[randUserIndex];

        int randUserID = userIDs.get(randUser);

        String tweet = "";
        for (int n = 0; n < wordsPerTweet; n++) {
            int randWordIndex = (int) Math.round(Math.random() * (dictionary.length -1));
            String randWord = dictionary[randWordIndex];
            if(n > 0){
                tweet += " ";
            }
            tweet += randWord;
        }

        return new Values(randUser, randUserID, tweet);
    }
}