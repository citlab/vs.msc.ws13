package de.tu_berlin.citlab.storm.topologies;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.operators.FilterOperator;
import de.tu_berlin.citlab.storm.operators.FilterUDF;
import de.tu_berlin.citlab.storm.operators.join.JoinOperator;
import de.tu_berlin.citlab.storm.operators.join.JoinPredicate;
import de.tu_berlin.citlab.storm.operators.join.NLJoin;
import de.tu_berlin.citlab.storm.operators.join.TupleProjection;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
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


public class RealTwitterStream {
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
		
		

		
		Window<Tuple, List<Tuple>> WINDOW_TYPE =new CountWindow<Tuple>(windowSize, slidingOffset);
		//new TimeWindow<Tuple>(windowSize, slidingOffset);
		
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweets", new TweetSource(), 1);
		
		// flatmap
		builder.setBolt("tweets_flat_words",
				new UDFBolt(
					new Fields("user_id", "word", "id"),  // output fields
					new IOperator(){
						@Override
						public void execute(List<Tuple> input, OutputCollector collector) {
							for(Tuple t : input){
								String[] words = t.getValueByField("msg").toString().split(" ");
								for( String word : words ){
									collector.emit( new Values( t.getValueByField("user_id"), word, t.getValueByField("id") ) );
								}//for
							}//for
							
						}// execute()
				}, 
				WINDOW_TYPE ), 1 )
				.shuffleGrouping("tweets");

		// filter bad words and assign significance
		builder.setBolt("bad_words_filter",
				new UDFBolt(
					new Fields("user_id", "word", "id", "significance"),  // output fields
					new IOperator(){
						
						@Override
						public void execute(List<Tuple> input, OutputCollector collector) {
							for(Tuple t : input){
								String word = t.getValueByField("word").toString().toLowerCase();
								
								// do not change
								if( STORAGE.badWords.containsKey(word) ){
									BadWord badWord = STORAGE.badWords.get(word);
									collector.emit( new Values( t.getValueByField("user_id"), word, t.getValueByField("id"), ""+badWord.significance ) );
								}//if
																		
							}//for
							
						}// execute()
				}, 
				WINDOW_TYPE ), 1 )
				.shuffleGrouping("tweets_flat_words");


		// update user statistics
		builder.setBolt("update_user_significance",
				new UDFBolt(
					new Fields("user_id" ),  // output fields
					new IOperator(){						
						@Override
						public void execute(List<Tuple> input, OutputCollector collector) {
							
							for(Tuple t : input){
								String userid = t.getValueByField("user_id").toString();
								String word = t.getValueByField("word").toString().toLowerCase();
								int significance = Integer.parseInt( t.getValueByField("significance").toString() );
								
								// user update in cassandra
								// user do not exists => add
								//    otherwise update
								STORAGE.updateUser(userid, word, significance);
								
								collector.emit( new Values( userid  ) );
																		
							}//for
							
						}// execute()
				}, 
				WINDOW_TYPE ), 1 )
				.shuffleGrouping("bad_words_filter");
		
		
		// update user statistics
		builder.setBolt("user_total_signifiance",
				new UDFBolt(
					new Fields("user_id", "total_significance" ),  // output fields
					new IOperator(){						
						@Override
						public void execute(List<Tuple> input, OutputCollector collector) {
							for(Tuple t : input){
								String userid = t.getValueByField("user_id").toString();
								Integer total_significance = 0;
								
								
								// user exists in cassandra ? => find
								if( STORAGE.users.containsKey(userid) ){
									total_significance = STORAGE.users.get(userid).total_significance;
								}
								collector.emit( new Values( userid, total_significance ) );
																		
							}//for
							
						}// execute()
				}, 
				WINDOW_TYPE ), 1 )
				.shuffleGrouping("update_user_significance");
		
		
		
		builder.setBolt(
			"significant_users",
			new UDFBolt(
				new Fields("user_id", "total_significance" ), // output
				new FilterOperator(
					new Fields("user_id", "total_significance" ), // input
					new FilterUDF() {
						public Boolean evaluate(Tuple tuple) {
							
							return (Integer) tuple.getValueByField("total_significance") > 1000001;
						}
					}
				)
			),
			1
		).shuffleGrouping("user_total_signifiance");
		
		
		
		
		JoinPredicate joinPredicate = new JoinPredicate() {
			public boolean evaluate(Tuple t1, Tuple t2) {
				return ((String)t1.getValueByField("user_id")).compareTo( (String)t2.getValueByField("user_id") ) == 0;
			}
		};
		
		
		TupleProjection projection = new TupleProjection(){
			public Values project(Tuple left, Tuple right) {
				
				return new Values(  left.getValueByField("user_id"),
									left.getValueByField("msg"),
									right.getValueByField("total_significance")									
								);
			}
		};
		
		// maybe at this point a different window-type makes sense
		// e.g. time based filter to make time correlated data
		
		builder.setBolt("significance_user_with_tweets",
				new UDFBolt(
					new Fields("user_id", "msg", "total_significance" ) , // no outputFields
					new JoinOperator( 	new NLJoin(), 
										joinPredicate, 
										projection, 
										"significant_users", "tweets" ), 
					WINDOW_TYPE,
					KeyConfigFactory.BySource()
				),
		1)	
		.shuffleGrouping("tweets")
		.shuffleGrouping("significant_users");

		
		builder.setBolt("cassandra_save_tweets",
				new UDFBolt(
						new Fields("user_id", "msg", "total_significance" ), 
					new IOperator() {
						public void execute(List<Tuple> tuples, OutputCollector collector ) {

							for( Tuple tweet: tuples) {
								// store in cassandra
								
								
								collector.emit(tweet.getValues() );
								
							}//for
						}
					},
					new CountWindow<Tuple>(windowSize, slidingOffset),
					KeyConfigFactory.ByFields("key1", "key2")
				),
			1).shuffleGrouping("spout");

		
		Config conf = new Config();
		conf.setDebug(true);

		conf.setMaxTaskParallelism(1);
		conf.setMaxSpoutPending(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sliding-count-window-group-by-test", conf,
				builder.createTopology());
	}
}
