package de.tu_berlin.citlab.storm.tick;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TickVerifierTopology {

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setBolt("bolt1", new TickVerifierBolt(1), 1);
		builder.setBolt("bolt2", new TickVerifierBolt(2), 1);
		builder.setBolt("bolt3", new TickVerifierBolt(3), 1);
		builder.setBolt("bolt4", new TickVerifierBolt(4), 1);
		builder.setBolt("bolt5", new TickVerifierBolt(5), 1);

		Config conf = new Config();
		conf.setDebug(false);

		conf.setMaxTaskParallelism(1);
		conf.setMaxSpoutPending(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("tick-reliability-test", conf,
				builder.createTopology());
	}

}
