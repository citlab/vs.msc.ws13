package de.tu_berlin.citlab.testsuite.testSkeletons;

import backtype.storm.tuple.Tuple;
import org.junit.Test;

import java.util.List;

/**
 * Created by Constantin on 13.03.14.
 */
abstract public class StandaloneTest<B extends BoltTest, O extends OperatorTest>
{
    private B boltTestDescr;
    private O opTestDescr;

    public StandaloneTest()
    {
        this.boltTestDescr = initBoltTestDescr();
        this.opTestDescr = initOpTestDescr();
        this.opTestDescr.initTestSetup(generateInputTuples());
    }

    protected abstract B initBoltTestDescr();
    protected abstract O initOpTestDescr();

    protected abstract List<Tuple> generateInputTuples();

    @Test
    public void opTestWordFlatMap()
    {
        opTestDescr.testOperator();
    }

    @Test
    public void boltTestWordFlatMap()
    {
        boltTestDescr.testUDFBolt();
    }
}
