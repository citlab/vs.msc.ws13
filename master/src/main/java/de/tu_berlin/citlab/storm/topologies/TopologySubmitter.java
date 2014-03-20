package de.tu_berlin.citlab.storm.topologies;

import de.tu_berlin.citlab.logging.LoggingConfigurator;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;

/**
 * Created by Constantin on 19.03.2014.
 */
public class TopologySubmitter
{
    private static AnalyzeTweetsTopology topology = new AnalyzeTweetsTopology();
//    private static TopologyCreation topology;

    public TopologySubmitter()
    {
        topology = new AnalyzeTweetsTopology();
    }


    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception
    {
        Config conf = new Config();

        if (args == null || args.length == 0) {
        	
        	// uncomment to enable DB logging
        	// LoggingConfigurator.activateDataBaseLogger();
        	
            conf.setDebug(true);

            conf.setMaxTaskParallelism(1);
            conf.setMaxSpoutPending(1);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("analyzte-twitter-stream", conf, topology.createTopology());
//                    new AnalyzeTweetsTopology().createTopology() );

            cluster.shutdown();
        }
        else{
        	LoggingConfigurator.activateDataBaseLogger();
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, topology.createTopology());
        }

    }
}
