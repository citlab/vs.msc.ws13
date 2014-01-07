package de.tu_berlin.citlab.storm.topologies;

import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import de.tu_berlin.citlab.storm.spouts.TwitterSpout;
import de.tu_berlin.citlab.twitter.InvalidTwitterConfigurationException;
import de.tu_berlin.citlab.twitter.TwitterConfiguration;
import de.tu_berlin.citlab.twitter.TwitterUserLoader;

public class TwitterLocalTestTopology {
	public static void main(String[] args) {

		// Setup up Twitter configuration
		Properties user = TwitterUserLoader.loadUserFromJar("twitter.config");
		String[] keywords = new String[] { "the", "it", "der", "die", "das" };
		String[] languages = new String[] { "en", "de" };
		String[] outputFields = new String[] { "user", "tweet", "date", "lang" };
		TwitterConfiguration config = new TwitterConfiguration(user, keywords,
				languages, outputFields);

		TopologyBuilder builder = new TopologyBuilder();
		try {
			builder.setSpout("twitterSpout", new TwitterSpout(config), 1);
		} catch (InvalidTwitterConfigurationException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(30000);
		cluster.killTopology("test");
		cluster.shutdown();
		
		
	}
}
