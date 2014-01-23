package de.tu_berlin.citlab.storm.topologies;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.operators.FilterOperator;
import de.tu_berlin.citlab.storm.operators.FilterUDF;
import de.tu_berlin.citlab.storm.operators.join.JoinFactory;
import de.tu_berlin.citlab.storm.operators.join.JoinOperator;
import de.tu_berlin.citlab.storm.operators.join.NestedLoopJoin;
import de.tu_berlin.citlab.storm.operators.join.TupleProjection;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.TimeWindow;
import de.tu_berlin.citlab.storm.window.Window;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TweetsFilterUsersWithBadWordsTotalTweetSignificanceTopology {
	private static final int windowSize = 10;
	private static final int slidingOffset = 10;
	

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		// prepare storage
		
		STORAGE.badWords.put("bombe", new BadWord("bombe", 100));
		STORAGE.badWords.put("nuklear", new BadWord("nuklear", 1000));
		STORAGE.badWords.put("anschlag", new BadWord("anschlag", 200));
		STORAGE.badWords.put("religion", new BadWord("religion", 100));
		STORAGE.badWords.put("macht", new BadWord("macht", 300));
		STORAGE.badWords.put("kampf", new BadWord("kampf", 300));
		
		

		
		Window<Tuple, List<Tuple>> WINDOW_TYPE =new CountWindow<Tuple>(windowSize, slidingOffset);
		//Window<Tuple, List<Tuple>> WINDOW_TYPE =new TimeWindow<Tuple>(1, 1);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweets", new TweetSource(), 1);
		
		// flatmap
		builder.setBolt("tweets_flat_words",
				new UDFBolt(
					new Fields("user_id", "tweet_id", "word"),  // output fields
					new IOperator(){
						public void execute(List<Tuple> input, OutputCollector collector) {
							for(Tuple t : input){
								String[] words = t.getValueByField("msg").toString().split(" ");
								for( String word : words ){
									collector.emit( new Values( t.getValueByField("user_id"), t.getValueByField("tweet_id"), word ) );
								}//for
							}//for
							
						}// execute()
				}, 
				WINDOW_TYPE ), 1 )
				.shuffleGrouping("tweets");
		
		
				// aggregate treetId significance
			builder.setBolt("aggregated_tweet_significance",
					new UDFBolt(
						new Fields("user_id", "tweet_id", "total_tweet_significance", "words" ), // output
						new IOperator(){						
							public void execute(List<Tuple> input, OutputCollector collector) {
								System.out.println("new window");
								for(Tuple t : input){
									System.out.println(t);
								}
							}// execute()
					}, 
					WINDOW_TYPE, 
					KeyConfigFactory.ByFields("user_id", "tweet_id" ) )
			, 1 ).shuffleGrouping("tweets_flat_words");
			
			
	
		Config conf = new Config();
		conf.setDebug(false);
		
		conf.setMaxTaskParallelism(1);
		conf.setMaxSpoutPending(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sliding-count-window-group-by-test", conf,
				builder.createTopology());
	}
}
