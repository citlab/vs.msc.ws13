package de.tu_berlin.citlab.storm.tests.twitter.operatorTests;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.operators.FilterOperator;
import de.tu_berlin.citlab.storm.operators.FilterUDF;
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
public class Op5_SignificantUsers extends OperatorTest implements OperatorTestMethods
{
	public Op5_SignificantUsers(String testName) {
	//TestName and inputFields:
		super(testName, new Fields("user_id", "total_significance"));
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
		IOperator filterUsers = new FilterOperator(
				new Fields("user_id", "total_significance" ), // input
				new FilterUDF() {
					public Boolean evaluate(Tuple tuple) {
						return (Integer)
								tuple.getValueByField("total_significance") > 100;
					}
				}
		);
		return filterUsers;
	}

	@Override
	public List<List<Object>> assertOutput(List<Tuple> inputTuples) {
		List<List<Object>> emptyList = new ArrayList<List<Object>>();
		return emptyList;
	}
}
