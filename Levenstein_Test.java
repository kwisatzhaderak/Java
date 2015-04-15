package com.auction.data.hadoop.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.test.Util;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by wrendall on 12/29/2014.
 */
public class TestDemLevString {

    private final BagFactory bagFactory = BagFactory.getInstance();
    private final TupleFactory tupleFactory = TupleFactory.getInstance();

    private Tuple inputBagTuple;
    private List<Tuple> addressDataTuples;
    private List<Tuple> propertyDataTuples;
    @Before
    public void creatInputTuple() throws IOException {
        //Util.getTuplesFromConstantTupleStrings(input)
        addressDataTuples = new ArrayList<Tuple>();
        propertyDataTuples = new ArrayList<Tuple>();

        //BUILD THE TRANSACTION DATA
        //<months, critical value, address, unix_time_of_transfer, dollar_value>
        //""
        //null
        //NULL
        //2318 OAKVIEW RD NE ATLANTA GA 30317
        //7665 W STENE DR LITTLETON CO 801286916
        //9900 CUDELL AVE CLEVELAND OH 441023613
        //3 WAYNE DR EAST COLUMBUS NJ 080229740
        //289 GRANT ST LEESPORT PA 19533
        addressDataTuples.add(Util.buildTuple(2,"7665 W STENE DR LITTLETON CO 801286916", 201401, 375000));
        addressDataTuples.add(Util.buildTuple(2,"NULL", 201401, 375000));
        addressDataTuples.add(Util.buildTuple(2,"7665 ", 201401, 375000));
        addressDataTuples.add(Util.buildTuple(2,"2318 OAKVIEW RD NE ATLANTA GA 30317", 201401, 375000));
        addressDataTuples.add(Util.buildTuple(2,"2318 OAKVIEW RD NE ATLANTA GA 30317", 201405, 225000));
        addressDataTuples.add(Util.buildTuple(2,"9900 CUDELL AVE CLEVELAND OH 441023613", 3, 10.0));
        addressDataTuples.add(Util.buildTuple(2,"3 WAYNE DR EAST COLUMBUS NJ 080229740", 1, 265000));
        addressDataTuples.add(Util.buildTuple(2,"3 WAYNE DR EAST COLUMBUS NJ 080229740", 201401, 10.0));
        addressDataTuples.add(Util.buildTuple(2,"289 GRANT ST LEESPORT PA 19533", 201401, 10.0));
        addressDataTuples.add(Util.buildTuple(2,"289 GRANT ST LEESPORT PA 19533", 201411, 400000));

        /*
        addressDataTuples.add( Util.buildTuple (200123.0, 4, 10));
        addressDataTuples.add( Util.buildTuple (300123.00002, 4, 1.1));
         */
        //ADD THE userDataTuple TUPLES TO A BAG
        /*DataBag userDataBag = bagFactory.newDefaultBag();
        for(Tuple t : addressDataTuples) {
            userDataBag.add(t);
        }
        */
        //BUILD THE PROPERTY DATA
        //<address, YearMonth, price>
        propertyDataTuples.add( Util.buildTuple ("3250 E PALMER WASILLA HWY WASILLA AK 996547216", 201401, 375000) );
        propertyDataTuples.add( Util.buildTuple ("2318 OAKVIEW RD NE ATLANTA GA 303172634", 201405, 225000) );
        propertyDataTuples.add( Util.buildTuple ("7170 PULLMAN PL", 20140123, 265000) );
        propertyDataTuples.add( Util.buildTuple ("7170 PULLMAN PL", 201403, 285000) );
        propertyDataTuples.add( Util.buildTuple ("7665 W STENE DRIVE LITTLETON CO 80128", 201401, 375000) );
        propertyDataTuples.add( Util.buildTuple ("7665 WEST STEINE DR LITTLETON CO 80128", 201401, 375000) );
        propertyDataTuples.add( Util.buildTuple ("7665 WEST STEINE DR LITTLETON CO 80128", 201401, 375000) );
        propertyDataTuples.add( Util.buildTuple ("3 WAYNE DR EAST COLUMBUS NJ 080229740", 201401, 265000) );
        propertyDataTuples.add( Util.buildTuple ("2921 WAINWRIGHT AVE MERCED CA 953402425", 201401, 265000) );
        propertyDataTuples.add( Util.buildTuple ("580 5TH AVE #1200 NEW YORK NY 100364728", 201401, 265000) );
        propertyDataTuples.add( Util.buildTuple ("289 GRANT AVE LEESPORT PA 195339522", 201401, 265000) );
        propertyDataTuples.add( Util.buildTuple ("289 GRANT AVE LEESPORT PA 195339522", 201407, 365000) );
        propertyDataTuples.add( Util.buildTuple ("7665 WEST STEINE DR LITTLETON CO 80128", 201401, 375000) );
        propertyDataTuples.add( Util.buildTuple ("289 GRANT AVE LEESPORT PA 195339522", 201412, 400000) );
        //ADD THE propertyData TUPLES TO A BAG
        DataBag propertyBag = bagFactory.newDefaultBag();
        for(Tuple t : propertyDataTuples) {
            propertyBag.add(t);
        }
        //ADD THE propertyBag TO EACH USER TUPLE
        for(Tuple t : addressDataTuples) {
            t.append(propertyBag);
        }
    }

//RESULTS SHOULD BE IN THE FORM: DATABAG: { TUPLE: (ADDRESS, DATE, VALUE) , SINGLETON-TUPLE: (COST-TO-CHANGE) }
    @Test
    public void testDemLevStringInput() throws Exception {
        inputBagTuple = addressDataTuples.get(0);
        EvalFunc<DataBag> fn = new DemLevStringSim();
        DataBag resultBag = fn.exec(inputBagTuple);

        Tuple t;
        Iterator<Tuple> resultTuples = resultBag.iterator();
        t = resultTuples.next();
        //Results come back inside tuples, so we have to cast expectations with that wrapper
        assertEquals("first value in input tuple should be '2'", 2, inputBagTuple.get(0));
        //2nd value in input tuple should be a string
        if (String.class.isInstance(inputBagTuple.get(1))) {
            assertEquals("2nd value in input tuple should be a string", 1, 1);
        } else {
            assertEquals("2nd value in input tuple should be a string", 1, 0);
        }
    }

    @Test
    public void testDemLevStringOutput() throws Exception {

        inputBagTuple = addressDataTuples.get(0);
        EvalFunc<DataBag> fn_0 = new DemLevStringSim();
        DataBag resultBag_0 = fn_0.exec(inputBagTuple);

        Tuple t_0;
        Iterator<Tuple> resultTuples_0 = resultBag_0.iterator();
        t_0 = resultTuples_0.next();
        //Results come back inside tuples, so we have to cast expectations with that wrapper
        assertEquals("first tuple in output should be 3 cost", 3, t_0.get(1));

        inputBagTuple = addressDataTuples.get(1);
        EvalFunc<DataBag> fn_1 = new DemLevStringSim();
        DataBag resultBag_1 = fn_1.exec(inputBagTuple);

        Tuple t_1;
        Iterator<Tuple> resultTuples_1 = resultBag_1.iterator();
        t_1 = resultTuples_1.next();
        assertEquals("2nd tuple in output should be 32 penalty", 32, t_1.get(1));
        /*
        assertEquals("first tuple in output should be '4.22474'", inputTuples.get(0), Math.log10(Math.abs(116900.0-100123.0)+1.0));
        assertEquals("second tuple in output should be '4.926003'", inputTuples.get(1), Math.log10(Math.abs(15790.0-100123.0)+1.0))

        assertEquals("first output tuple should have rank 3", t.get(3), Math.log10(Math.abs(116900.0-100123.0)+1.0));
        t = resultTuples.next();
        assertEquals("second tuple in output should be 'Rhinoceros'", inputTuples.get(3), t);
        assertEquals("first output tuple should have rank 2", t.get(3), 2);
        t = resultTuples.next();
        assertEquals("third tuple in output should be 'zebra'", inputTuples.get(1), t);
        assertEquals("first output tuple should have rank 1", t.get(3), 1);
        assertEquals()
        */
    }
}

