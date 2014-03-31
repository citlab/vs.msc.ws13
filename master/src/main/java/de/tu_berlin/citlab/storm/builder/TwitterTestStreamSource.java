package de.tu_berlin.citlab.storm.builder;

import de.tu_berlin.citlab.storm.spouts.TwitterGeneratorSpout;
import de.tu_berlin.citlab.storm.spouts.TwitterSpout;
import de.tu_berlin.citlab.storm.spouts.UDFSpout;
import de.tu_berlin.citlab.storm.udf.UDFOutput;
import de.tu_berlin.citlab.twitter.InvalidTwitterConfigurationException;
import de.tu_berlin.citlab.twitter.TwitterConfiguration;
import de.tu_berlin.citlab.twitter.TwitterUserLoader;

import java.util.Properties;

public class TwitterTestStreamSource extends StreamSource {
    private final Properties user = TwitterUserLoader.loadUserFromJar("twitter.config");
    private UDFSpout spout;

    public TwitterTestStreamSource(StreamBuilder builder, String[] users, String[] dict,
                                   int wordsPerTweet, int tweetsPerSecond)
    {
        super(builder);
        spout = new TwitterGeneratorSpout(users, dict, wordsPerTweet, tweetsPerSecond);
        getStreamBuilder().getTopologyBuilder().setSpout(getNodeId(), spout );
    }

    @Override
    public UDFOutput getUDFOutput(){
        return spout;
    }
}