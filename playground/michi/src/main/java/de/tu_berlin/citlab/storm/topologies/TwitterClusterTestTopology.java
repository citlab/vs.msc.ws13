package de.tu_berlin.citlab.storm.topologies;

import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import de.tu_berlin.citlab.storm.spouts.TwitterSpout;
import de.tu_berlin.citlab.twitter.InvalidTwitterConfigurationException;
import de.tu_berlin.citlab.twitter.TwitterConfiguration;
import de.tu_berlin.citlab.twitter.TwitterUserLoader;

public class TwitterClusterTestTopology {
	public static void main(String[] args) {

		if (args.length != 1) {
			System.err.println("You must pass the topology name as argument");
			System.exit(-1);
		}

		// Setup up Twitter configuration
		Properties user = TwitterUserLoader.loadUserFromJar("twitter.config");
		String[] keywords = new String[] { "the", "it", "der", "die", "das" };
		String[] languages = new String[] { "en", "de" };
		String[] outputFields = new String[] { "id", "user", "tweet", "date",
				"lang" };
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
		conf.setMaxSpoutPending(5000);
		try {
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}

	}
}
