package de.tu_berlin.citlab.testsuite.helpers;


import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.testsuite.mocks.TupleMock;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by Constantin on 1/22/14.
 */
public final class TupleMockFactory
{

    public static List<Tuple> generateTupleList_ByFields(Values[] values, Fields fields)
    {
        //A maximum of values.length tickTuples will also be added to the TupleList:
        int randTickTupleCount = (int) Math.round(Math.random() * values.length);

        //Initialize the Tuple-List:
        int tupleListSize = values.length + randTickTupleCount;
        List<Tuple> tupleList = new ArrayList<Tuple>(tupleListSize);

        //First add the Value-Tuples:
        for(int n = 0 ; n < values.length ; n++){
            Values actVals = values[n];
            tupleList.add(TupleMock.mockTupleByFields(actVals, fields));
        }

        //Then add the tick-Tuples at random positions:
        //TODO.

        return tupleList;
    }
}
