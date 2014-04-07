package de.tu_berlin.citlab.testsuite.helpers;


import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.testsuite.mocks.TupleMock;

import java.util.ArrayList;
import java.util.List;


/**
 * Helper Data-Class for the {@link de.tu_berlin.citlab.testsuite.testSkeletons.TopologyTest TopologyTest}. <br />
 * There are two ways to construct a BoltEmission Data-Container:
 * <p>
 *     First: Manual generation in a TopologyTest-case: <br />
 *     Just insert an {@link java.util.ArrayList} of {@link TupleMock TupleMocks}. <br />
 *     <em>(Needed for the {@link de.tu_berlin.citlab.testsuite.testSkeletons.TopologyTest#defineFirstBoltsInput() TopologyTest.defineFirstBoltsInput()}-method).</em>
 * </p>
 * <p>
 *     Second: Automatic generation via a {@link de.tu_berlin.citlab.testsuite.testSkeletons.BoltTest BoltTest}: <br />
 *     Each {@link de.tu_berlin.citlab.testsuite.testSkeletons.BoltTest BoltTest} returns a <b>BoltEmission</b>
 *     with the given <b>Output-Fields</b> and the <b>Output-Values</b>, emitted by the
 *     {@link de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock OutputCollectorMock} inside the BoltTest.
 * </p>
 * @author Constantin on 12.03.14.
 */
public class BoltEmission
{
    public final List<Tuple> tupleList;

    public BoltEmission(ArrayList<Tuple> tupleInput)
    {
        this.tupleList = tupleInput;
    }

    public BoltEmission(List<List<Object>> outputEmission, Fields outputFields)
    {
        this.tupleList = new ArrayList<Tuple>(outputEmission.size());
        int n = 0;
        for (List<Object> outputValObjs : outputEmission) {
            Values outputVals = new Values();
            for (Object outVal : outputValObjs) {
                outputVals.add(outVal);
            }
            Tuple actTuple = TupleMock.mockTupleByFields(outputVals, outputFields);
            this.tupleList.add(actTuple);

            if((n % 5) == 0){
                Tuple tickTuple = TupleMock.mockTickTuple();
                this.tupleList.add(tickTuple);
            }

            n++;
        }
    }
}
