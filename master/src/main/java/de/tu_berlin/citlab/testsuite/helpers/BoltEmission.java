package de.tu_berlin.citlab.testsuite.helpers;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.testsuite.mocks.TupleMock;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Constantin on 12.03.14.
 * Helper Data-Class for the {@link de.tu_berlin.citlab.testsuite.testSkeletons.TopologyTest}.
 * <p>
 *     Each {@link de.tu_berlin.citlab.testsuite.testSkeletons.BoltTest} returns a <b>BoltEmission</b>
 *     to with the given <b>Output-Fields</b> and the <b>Output-Values</b>, emitted by the
 *     {@link de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock} inside the BoltTest.
 * </p>
 * <p>
 *     For an BoltTest, it is important to define ... in it's
 *     {@link de.tu_berlin.citlab.testsuite.testSkeletons.BoltTest#generateInputTuples()} to
 *     use the input of the predecessing Bolt in the topology.
 * </p>
 */
public class BoltEmission
{
    public final List<Tuple> tupleList;
    public final Fields outputFields;

    public BoltEmission(ArrayList<Tuple> tupleInput, Fields outputFields)
    {
        this.tupleList = tupleInput;
        this.outputFields = outputFields;
    }

    public BoltEmission(List<List<Object>> outputEmission, Fields outputFields)
    {
        this.tupleList = new ArrayList<Tuple>(outputEmission.size());
        for (List<Object> outputValObjs : outputEmission) {
            Values outputVals = new Values();
            for (Object outVal : outputValObjs) {
                outputVals.add(outVal);
            }
            Tuple actTuple = TupleMock.mockTupleByFields(outputVals, outputFields);
            this.tupleList.add(actTuple);
        }
        this.outputFields = outputFields;
    }
}
