package de.tu_berlin.citlab.storm.tests.twitter.operatorTests;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.operators.join.JoinOperator;
import de.tu_berlin.citlab.storm.operators.join.NestedLoopJoin;
import de.tu_berlin.citlab.storm.operators.join.TupleProjection;
import de.tu_berlin.citlab.storm.tests.twitter.helpers.BadWord;
import de.tu_berlin.citlab.storm.tests.twitter.helpers.STORAGE;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.mocks.TupleMock;
import de.tu_berlin.citlab.testsuite.testSkeletons.OperatorTest;
import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.OperatorTestMethods;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Constantin on 3/10/14.
 */
public class Op6_SignUserWithTweets extends OperatorTest implements OperatorTestMethods
{
	public Op6_SignUserWithTweets(String testName) {
		super(testName, new Fields("user_id", "msg", "total_significance"));
	}

	@Override
	public List<Tuple> generateInputTuples() {
		List<Tuple> tupleList = new ArrayList<Tuple>();
		int userID = 1;
		int id = 0;
		tupleList.add(TupleMock.mockTupleByFields(new Values(userID, "hey", id), this.getInputFields()));
		tupleList.add(TupleMock.mockTupleByFields(new Values(userID, "leute", id), this.getInputFields()));
		tupleList.add(TupleMock.mockTupleByFields(new Values(userID, "heute.", id), this.getInputFields()));
		return tupleList;
	}

	@Override
	public IOperator initOperator(List<Tuple> inputTuples) {

		TupleProjection projection = new TupleProjection(){
			public Values project(Tuple left, Tuple right) {
				return new Values(  left.getValueByField("user_id"),
						left.getValueByField("msg"),
						right.getValueByField("total_significance")
				);
			}
		};

		IOperator joinUsers = new JoinOperator(new NestedLoopJoin(),
										KeyConfigFactory.compareByFields(new Fields("user_id")),
										projection,
										"significant_users", "tweets" );
		return joinUsers;
	}

	@Override
	public List<List<Object>> assertOutput(List<Tuple> inputTuples) {
		List<List<Object>> emptyList = new ArrayList<List<Object>>();
		return emptyList;
	}
}
