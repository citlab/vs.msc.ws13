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

class BadWord {
	public BadWord(String word, int significance){
		this.word =word;
		this.significance = significance;
	}
	public String word;
	public int significance;
}

class User{
	public static HashMap<String, Integer> wordStaticstics = new HashMap<String, Integer>();
	public String userId;
	public int total_significance;	
	public User(String u,int s){
		userId=u;
		total_significance=s;
	} //User
	public void updateWord(String w, int significance){
		this.total_significance += significance;
		if( wordStaticstics.containsKey(w) ){
			wordStaticstics.put( w, wordStaticstics.get(w).intValue() + 1 );
		} else {
			wordStaticstics.put( w, 1 );
		}
	}
}

class STORAGE {
	public static HashMap<String, User> users = new HashMap<String, User>();
	public static HashMap<String, BadWord> badWords = new HashMap<String, BadWord>();
	
	public static void updateUser( String userId, String word, int significance ){
		if( users.containsKey(userId) ){
			users.get(userId).updateWord( word, significance);
		} else{
			User u = new User(userId, 0);
			u.updateWord( word, significance);
			users.put(userId, u );
		}
	}
}


class TweetSource extends BaseRichSpout {
	
	private static final long serialVersionUID = -7374814904789368773L;
	
	private String[] user_ids = new String[] { "1", "2" , "3", "4", "5","6", "7","8", "9", "10" };
	private String[] user_messages = new String[] { "hey leute heute war angela total witzlos.", 
													"ist noch immer in den trends... Die Community is doch verrückt xD",
													"das wetter ist sooo toll",
													"storm ist interessant",
													"wat für ne sexbombe",
													"bomben bauen macht spass und kann ich jedem beibringen" };
	private int currentId = 0;

	int _id = 0;

	SpoutOutputCollector _collector;

	@Override
	public void ack(Object msgId) {
	}

	@Override
	public void fail(Object msgId) {
	}

	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

	public void nextTuple() {
		Utils.sleep(1);
		_collector.emit(new Values(	user_ids[currentId++ % user_ids.length],
									user_messages[currentId++ % user_messages.length],
									_id));
		_id++;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user_id", "msg", "id"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}


public class ImportantTweetWords {
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
		
		

		
		Window<Tuple, List<Tuple>> WINDOW_TYPE =new TimeWindow<Tuple>(windowSize, slidingOffset);
		//new TimeWindow<Tuple>(windowSize, slidingOffset);
		
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweets", new TweetSource(), 1);
		
		// flatmap
		builder.setBolt("tweets_flat_words",
				new UDFBolt(
					new Fields("user_id", "word", "id"),  // output fields
					new IOperator(){
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
						public void execute(List<Tuple> input, OutputCollector collector) {
							for(Tuple t : input){
								String word = t.getValueByField("word").toString().toLowerCase();
								
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
						public void execute(List<Tuple> input, OutputCollector collector) {
							
							for(Tuple t : input){
								String userid = t.getValueByField("user_id").toString();
								String word = t.getValueByField("word").toString().toLowerCase();
								int significance = Integer.parseInt( t.getValueByField("significance").toString() );
								
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
						public void execute(List<Tuple> input, OutputCollector collector) {
							for(Tuple t : input){
								String userid = t.getValueByField("user_id").toString();
								Integer total_significance = 0;
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

		
		

		
		Config conf = new Config();
		conf.setDebug(false);
		
		conf.setMaxTaskParallelism(1);
		conf.setMaxSpoutPending(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sliding-count-window-group-by-test", conf,
				builder.createTopology());
	}
}
