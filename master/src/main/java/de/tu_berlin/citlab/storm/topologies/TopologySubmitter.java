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
//    private static AnalyzeTweetsTopology topology;
    private static TopologyCreation topology;

    public TopologySubmitter()
    {
        
    }
    
    private static void setTopology() {
    	topology = new AnalyzeTweetsTopology();
    }


    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception
    {
        if (args == null || args.length == 0) {
        	// uncomment to enable DB logging
        	LoggingConfigurator.activateDataBaseLogger();
        	
        	setTopology();
            Config conf = new Config();
        	
            conf.setDebug(true);

            conf.setMaxTaskParallelism(1);
            conf.setMaxSpoutPending(1);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("analyzte-twitter-stream", conf, topology.createTopology());
//                    new AnalyzeTweetsTopology().createTopology() );

            cluster.shutdown();
        }
        else{
        	String session = "";
        	// TODO: read session from args, then set session field, then add session as parameter to this call:
        	// session = args[1]; // or something like that
        	LoggingConfigurator.activateDataBaseLogger(/* session */);
        	setTopology();
            Config conf = new Config();
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, topology.createTopology());
        }

    }
}
