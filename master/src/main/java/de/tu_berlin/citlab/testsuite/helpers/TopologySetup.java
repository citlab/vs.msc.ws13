package de.tu_berlin.citlab.testsuite.helpers;

import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.testsuite.testSkeletons.OperatorTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Internal Data-Class for a {@link de.tu_berlin.citlab.testsuite.testSkeletons.TopologyTest TopologyTest}.
 * <p>
 *     Creates the topology-Setup via a chain of {@link BoltTestConfig BoltTestConfigs} and uses internally
 *     TestName -> BoltTestConfig Mappings to define this topology-chain and prepare it to be used in the
 *     TopologyTest class.
 * </p>
 * @author Constantin on 12.03.14.
 */
public class TopologySetup
{
    public final List<String> boltNameOrder;
	public final Map<String, UDFBolt> boltTests;
    public final Map<String, OperatorTest> boltOPTests;
	public final Map<String, Integer> boltSleepTimer;


    public TopologySetup(String topologyTestName, List<BoltTestConfig> topology)
    {
        this.boltNameOrder = new ArrayList<String>(topology.size());
        this.boltOPTests = new HashMap<String, OperatorTest>(topology.size());
        this.boltTests = new HashMap<String, UDFBolt>(topology.size());
		this.boltSleepTimer = new HashMap<String, Integer>(topology.size());

        for (BoltTestConfig actTestConfig : topology) {
            final String testName = topologyTestName +"/"+ actTestConfig.testName;
            final UDFBolt testBolt = actTestConfig.testBolt;
			final int boltSleepTimer = actTestConfig.boltSleepTimer;
            final List<List<Object>> assertedOutput = actTestConfig.assertedOutput;

            this.boltNameOrder.add(testName);

            OperatorTest opTest = new OperatorTest(testName) {
                @Override
                public IOperator initOperator() {
                    return testBolt.getOperator();
                }

                @Override
                public List<List<Object>> assertOperatorOutput(List<Tuple> inputTuples) {
                    return assertedOutput;
                }
            };

			this.boltTests.put(testName, testBolt);
            this.boltOPTests.put(testName, opTest);
			this.boltSleepTimer.put(testName, boltSleepTimer);
        }
    }
}
