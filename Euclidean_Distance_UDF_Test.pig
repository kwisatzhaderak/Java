package com.auction.data.hadoop.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.test.Util;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;



/**
 * Created by wrendall on 8/21/2014.
 */
public class TestEucDist {

    private final BagFactory bagFactory = BagFactory.getInstance();
    private final TupleFactory tupleFactory = TupleFactory.getInstance();

    private Tuple inputTupleWithTuples;
    private Tuple inputTupleWithBags;
    private Tuple inputTupleWithBoth;
    private DataBag distanceBag1;
    private DataBag distanceBag2;
    private DataBag distanceBag3;
    private Tuple distanceTuple1;
    private Tuple distanceTuple2;
    private Tuple distanceTuple3;

    @Before
    public void createInputTuple() throws IOException {
        inputTupleWithTuples = TupleFactory.getInstance().newTuple();
        inputTupleWithBags = TupleFactory.getInstance().newTuple();
        inputTupleWithBoth = TupleFactory.getInstance().newTuple();
        distanceBag1 = BagFactory.getInstance().newDefaultBag();
        distanceBag2 = BagFactory.getInstance().newDefaultBag();
        distanceBag3 = BagFactory.getInstance().newDefaultBag();
        distanceTuple1 = TupleFactory.getInstance().newTuple();
        distanceTuple2 = TupleFactory.getInstance().newTuple();
        distanceTuple3 = TupleFactory.getInstance().newTuple();
        //CREATE AN INPUT TUPLE OF THE FORM:
        //<TUPLE, TUPLE,...>
        //<BAG, BAG,...>

        distanceBag1.add(tupleFactory.newTuple());
        distanceBag1.add(Util.buildTuple(10.0));
        distanceBag1.add(Util.buildTuple(5.0));
        distanceBag1.add(Util.buildTuple(0.0));
        distanceBag1.add(Util.buildTuple(10.0));

        distanceBag2.add(tupleFactory.newTuple());
        distanceBag2.add(Util.buildTuple(1.10));
        distanceBag2.add(Util.buildTuple(2.9133));
        distanceBag2.add(Util.buildTuple(9.01234));
        distanceBag2.add(Util.buildTuple(.1));

        distanceTuple1.append(null);
        distanceTuple1.append(Util.buildTuple(10.0));
        distanceTuple1.append(Util.buildTuple(11.0));
        distanceTuple1.append(Util.buildTuple(3.0123));
        distanceTuple1.append(Util.buildTuple(10.0));

        distanceTuple2.append(tupleFactory.newTuple());
        distanceTuple2.append(Util.buildTuple(10.0));
        distanceTuple2.append(Util.buildTuple(2.9133));
        distanceTuple2.append(Util.buildTuple(-1.0));
        distanceTuple2.append(Util.buildTuple(0.1));

        //List<Tuple> tupleList = new ArrayList<Tuple>();
        //DataBag HaversineBag1 = BagFactory.getInstance().newDefaultBag(List<Tuple> tupleList = A tupleFactory.newTuple(1662.0587684017178),tupleFactory.newTuple(51.48037746225796),tupleFactory.newTuple(2250.8752216433504))

        //ADD THE tuples and bags to a tuple to test each case
        //Tuple inputTupleWithBags = TupleFactory.getInstance().newTuple();
        for ( int i = 0; i < 6; i++) {
            if (i % 2 == 1) {
                inputTupleWithBags.append(distanceBag1);
            } else {
                inputTupleWithBags.append(distanceBag3);
            }
        }

       // Tuple inputTupleWithTuples = TupleFactory.getInstance().newTuple();
        for ( int j = 0; j < 6; j++) {
            if (j % 2 == 1) {
                inputTupleWithTuples.append(distanceTuple1);
            } else {
                inputTupleWithTuples.append(distanceTuple3);
            }
        }

        //
        for ( int m = 0; m < 6; m++) {
            if (m % 2 ==1) {
                inputTupleWithBoth.append(distanceBag2);
            } else {
                inputTupleWithBoth.append(distanceTuple2);
            }
        }
    }



    @Test
    public void BagTest() throws Exception {
        EvalFunc<DataBag> fn = new NTermEuclideanDistance();
        DataBag resultBag = fn.exec(inputTupleWithBags);

        Tuple t;
        Iterator<Tuple> resultTuples = resultBag.iterator();
        t = resultTuples.next();
        assertEquals("first value in output tuple should be 0.0 because input tuple is empty", 0.0, t.get(0));
        t = resultTuples.next();
        assertEquals("2nd value in input tuple should be '17.320508075688775 because we gave 3 null and 10.0 each'", 17.320508075688775, t.get(0));
        t = resultTuples.next();
        assertEquals("second tuple in output should be '8.660254037844387' because we gave 3 null and 5.0 each", 8.660254037844387, t.get(0));

    }
    @Test
    public void TupleTest() throws Exception {
        EvalFunc<DataBag> fn = new NTermEuclideanDistance();
        DataBag resultBag = fn.exec(inputTupleWithTuples);

        Tuple t;
        Iterator<Tuple> resultTuples = resultBag.iterator();
        t = resultTuples.next();
        assertEquals("first tuple in output should be 0.0 because input databags are empty", 0.0, t.get(0));
        t = resultTuples.next();
        assertEquals("second tuple in output should be '17.320508075688775' because we gave 3 null and 3 10 each", 17.320508075688775, t.get(0));
        t = resultTuples.next();
        assertEquals("third tuple in output should be '19.05255888325765'because we gave 3 null and 3 11 each", 19.05255888325765, t.get(0));
    }

    @Test
    public void MixedTest() throws Exception {
        EvalFunc<DataBag> fn = new NTermEuclideanDistance();
        DataBag resultBag = fn.exec(inputTupleWithBoth);

        Tuple t;
        Iterator<Tuple> resultTuples = resultBag.iterator();
        t = resultTuples.next();
        assertEquals("first tuple in output should be 0.0 because input databags are empty", 0.0, t.get(0));
        t = resultTuples.next();
        assertEquals("second tuple in output should be '17.320508075688775'", 17.4249820659879, t.get(0));
        t = resultTuples.next();
        assertEquals("third tuple in output should be '7.136098467650234'", 7.136098467650234, t.get(0));
    }
}

