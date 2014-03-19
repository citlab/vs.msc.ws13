package de.tu_berlin.citlab.testsuite.helpers;


import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.testsuite.mocks.TupleMock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by Constantin on 1/22/14.
 */
public final class TupleMockFactory
{

    public static ArrayList<Tuple> generateTupleList_ByFields(Values[] values, Fields fields)
    {
        //A maximum of values.length tickTuples will also be added to the TupleList:
        int randTickTupleCount = (int) Math.round(Math.random() * values.length);

        //Initialize the Tuple-List:
        int tupleListSize = values.length + randTickTupleCount;
        ArrayList<Tuple> tupleList = new ArrayList<Tuple>(tupleListSize);

        //First add the Value-Tuples:
        for(int n = 0 ; n < values.length ; n++){
            Values actVals = values[n];
            tupleList.add(TupleMock.mockTupleByFields(actVals, fields));
        }
        return tupleList;
    }

    /**
     *
     * @param users
     * @param dictionary
     * @param wordsPerTweet
     * @param tupleCount
     * @param tickTupleRatio
     * @return
     */
	public static ArrayList<Tuple> generateTwitterTuples(String[] users, String[] dictionary, int wordsPerTweet, int tupleCount, int tickTupleRatio)
	{
		Fields inputFields = new Fields("user", "tweet_id", "tweet");
		Map<String, Integer> userIDs = new HashMap<String, Integer>(users.length);
		initUserIDs(userIDs, users);
        int tickTupleCount = (tickTupleRatio == 0) ? 0 : tupleCount/tickTupleRatio;
		ArrayList<Tuple> tupleOutput = new ArrayList<Tuple>(tupleCount + tickTupleCount);

		for (int i = 0; i < tupleCount; i++) {

			Values actTupleVals = generateTweet(userIDs, users, dictionary, wordsPerTweet);
			Tuple actTuple = TupleMock.mockTupleByFields(actTupleVals, inputFields);
			tupleOutput.add(actTuple);


            if(tickTupleRatio > 0) {
                if ((i % tickTupleRatio) == 0) {
                    Tuple tickTuple = TupleMock.mockTickTuple();
                    tupleOutput.add(tickTuple);
                }
            }
		}

		return tupleOutput;
	}


	private static void initUserIDs(Map<String, Integer> userIDs, String[] users) {
		for (String actUser : users) {
			int randID = (int) Math.round(Math.random() * 10000);
			userIDs.put(actUser, randID);
		}
	}

	private static Values generateTweet(Map<String,Integer> userIDs, String[] users, String[] dictionary, int wordsPerTweet)
	{
		int randUserIndex = (int) Math.round(Math.random() * (users.length -1));
		String randUser = users[randUserIndex];

		int randUserID = userIDs.get(randUser);

		String tweet = "";
		for (int n = 0; n < wordsPerTweet; n++) {
			int randWordIndex = (int) Math.round(Math.random() * (dictionary.length -1));
			String randWord = dictionary[randWordIndex];
			if(n > 0){
				tweet += " ";
			}
			tweet += randWord;
		}

		return new Values(randUser, randUserID, tweet);
	}
}
