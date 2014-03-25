package de.tu_berlin.citlab.storm.topologies;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.builder.StreamBuilder;
import de.tu_berlin.citlab.storm.builder.StreamNode;
import de.tu_berlin.citlab.storm.builder.StreamSource;
import de.tu_berlin.citlab.storm.builder.TwitterStreamSource;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.helpers.TupleHelper;
import de.tu_berlin.citlab.storm.operators.Filter;
import de.tu_berlin.citlab.storm.operators.FlatMapper;
import de.tu_berlin.citlab.storm.operators.Mapper;
import de.tu_berlin.citlab.storm.operators.Reducer;
import de.tu_berlin.citlab.storm.operators.join.TupleProjection;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kay on 3/25/14.
 */
public class AnalyzeTweetsTopologyWithStreamBuilder implements TopologyCreation {

    @Override
    public StormTopology createTopology() {
        try{
            List<Tuple> badWordJoinSide = new ArrayList<Tuple>();

            badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("google", new Long(1))) );
            badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("microsoft", new Long(1) )) );
            badWordJoinSide.add( TupleHelper.createStaticTuple(new Fields("word", "significance"), new Values("facebook", new Long(1) )) );


            String[] keywords = new String[] {"der", "die","das","wir","ihr","sie", "dein", "mein", "facebook", "google", "twitter" };
            String[] languages = new String[] {"de"};
            String[] outputfields = new String[] {"user", "tweet_id", "tweet"};

            StreamBuilder stream = new StreamBuilder();
            StreamSource tweets = new TwitterStreamSource(stream).subscribe(keywords, languages, outputfields);

            StreamNode badWords =
                tweets.flapMap(new FlatMapper() {
                        @Override
                        public List<List<Object>> flatMap(Tuple tuple) {
                            String[] words = tuple.getStringByField("tweet")
                                    .replaceAll("[^a-zA-Z0-9 ]", "").split(" ");
                            List<List<Object>> result = new ArrayList<>();
                            for (String word : words) {
                                result.add(new Values(tuple.getValueByField("user"),
                                        tuple.getValueByField("tweet_id"),
                                        word.trim().toLowerCase()));
                            }
                            return result;
                        }
                    },
                    new Fields("user", "tweet_id", "word"));

            // prepare data to store in cassandra
            badWords.map( new Mapper() {
                            @Override
                            public List<Object> map(Tuple tuple) {
                                return new Values(tuple.getValueByField("word"), new Long(1) );
                            }
                        },
                        new Fields( "word", "count" ));

            badWords.join(KeyConfigFactory.BySource(),
                    badWordJoinSide.iterator(),
                    TupleProjection.project(new Fields("user", "tweet_id", "word"), new Fields("significance")),
                    new Fields("user", "tweet_id", "word", "significance"))

                   .reduce(new Fields("user", "tweet_id"),
                           new Reducer<Long>() {
                               @Override
                               public Long reduce(Long value, Tuple tuple ){
                                                  return tuple.getLongByField("significance")+value;
                           }})
                    .filter( new Filter(){
                        @Override
                        public Boolean predicate(Tuple t) {
                            if( t.getLongByField("significance") >= 1 ){
                                return true;
                            } else {
                                return false;
                            }
                        }},
                        new Fields( "user", "tweet_id", "significance" ));

        }catch(Exception e ){}

        return null;
    }
}
