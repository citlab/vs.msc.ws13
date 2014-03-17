package de.tu_berlin.citlab.testsuite.testSkeletons;


import de.tu_berlin.citlab.testsuite.testSkeletons.interfaces.StandaloneTestMethods;
import org.junit.Test;


/**
 * <p>
 *     The StandaloneTest is an <em><b>abstract Test-Skeleton</b></em> whose implementation class is representing a
 *     test case of the {@link BoltTest} and the {@link OperatorTest} at once.
 * </p>
 * <p>
 *     In order to sucessfully pass such a test, the {@link BoltTest#assertWindowedOutput(java.util.List)} and
 *     the {@link OperatorTest#assertOperatorOutput(java.util.List)} are equal to the respective output of the
 *     {@link de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock OutputCollector}'s emission in both cases.
 * </p>
 * <p>
 *     In contrary to the {@link BoltTest} and the {@link OperatorTest}, implementations of this test-skeleton are
 *     fully defining a testing environment. In regards to an environment, only {@link StandaloneTest StandaloneTests}
 *     and {@link TopologyTest TopologyTests} are fully conform with the testing compliance.
 * </p>
 *
 * @author Constantin on 13.03.14.
 */
abstract public class StandaloneTest<B extends BoltTest, O extends OperatorTest> implements StandaloneTestMethods<B,O>
{

/* Global Private Constants: */
/* ========================= */

    private final B boltTestDescr;
    private final O opTestDescr;


/* The Constructor: */
/* ================ */

    public StandaloneTest()
    {
        this.boltTestDescr = initBoltTestDescr();
        this.opTestDescr = initOpTestDescr();
        this.opTestDescr.initTestSetup(generateInputTuples());
        this.boltTestDescr.initTestSetup(generateInputTuples());
    }



/* Public JUnit Methods: */
/* ===================== */

    @Test
    public void opTestWordFlatMap()
    {
        opTestDescr.testOperator();
    }

    @Test
    public void boltTestWordFlatMap()
    {
        boltTestDescr.testUDFBolt(setSleepTimerBetweenTuples());
    }
}
