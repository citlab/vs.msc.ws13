package de.tu_berlin.citlab.storm.builder;

import backtype.storm.tuple.Fields;
import de.tu_berlin.citlab.storm.spouts.TwitterSpout;
import de.tu_berlin.citlab.storm.spouts.UDFSpout;
import de.tu_berlin.citlab.twitter.InvalidTwitterConfigurationException;
import de.tu_berlin.citlab.twitter.TwitterConfiguration;
import de.tu_berlin.citlab.twitter.TwitterUserLoader;

import java.util.Properties;

/**
 * Created by kay on 3/24/14.
 */
public class TwitterStreamSource extends StreamSource {
    private final Properties user = TwitterUserLoader.loadUser("twitter.config");
    private UDFSpout spout;

    public TwitterStreamSource(StreamBuilder builder) {
        super(builder);
    }

    public StreamSource subscribe ( String[] keywords, String[] languages, String[] outputfields ) throws InvalidTwitterConfigurationException {
        TwitterConfiguration config = new TwitterConfiguration(user, keywords, languages, outputfields);
        spout = new TwitterSpout(config, new Fields(outputfields));
        getStreamBuilder().getTopologyBuilder().setSpout(getNodeId(), spout );
        return this;
    }

}