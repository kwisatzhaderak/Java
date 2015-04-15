/**
 * Created by wrendall on 8/19/2014.
 */

/**
 * Created by wrendall on 8/19/2014.
 */

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
import java.util.Arrays;



//build input data

public class TestTypeMatch {

    private final BagFactory bagFactory = BagFactory.getInstance();
    private final TupleFactory tupleFactory = TupleFactory.getInstance();

    private Tuple inputBagTuple;
    private List<Tuple> inputTuples;
    private List<Tuple> userDataTuples;
    private List<Tuple> propertyDataTuples;
    private Tuple userTypeTuple;
    @Before
    public void creatInputTuple() throws IOException {
        //Util.getTuplesFromConstantTupleStrings(input)
        inputTuples = new ArrayList<Tuple>();
        userDataTuples = new ArrayList<Tuple>();
        propertyDataTuples = new ArrayList<Tuple>();

        //BUILD THE USER DATA
        //<user_type_preference, zero_index_of_type_in_property_tuple, nonmatch_penalty>
        List<String> PropertyTypes = Arrays.asList("Multi Family","","","","");
        userTypeTuple = tupleFactory.newTuple(PropertyTypes);
        userDataTuples.add( Util.buildTuple (userTypeTuple, 3, 10.0));
        /*
        userDataTuples.add( Util.buildTuple (200123.0, 4, 10));
        userDataTuples.add( Util.buildTuple (300123.00002, 4, 1.1));
         */
        //ADD THE userDataTuple TUPLES TO A BAG
        /*DataBag userDataBag = bagFactory.newDefaultBag();
        for(Tuple t : userDataTuples) {
            userDataBag.add(t);
        }
        */
        //BUILD THE PROPERTY DATA
        //<property_id, lat, lon, assettype, reserve, likelihoodtosell, producttype, state, bidenddt, bidstartdt, startingbid, city, systemauctionid, auctioncode>
        propertyDataTuples.add( Util.buildTuple (1668172,41.519924,-87.750079,"Multi Family",116900.0,"","REO","IL","2014-08-22 09:25:40.0","2014-08-19 03:30:00.0",25000,"MATTESON",14140,"O-469") );
        propertyDataTuples.add( Util.buildTuple (1668179,42.649945,-83.261367,"Duplex",15790.0,0.01,"REO","MI","2014-08-22 10:27:30.0","2014-08-19 03:30:00.0",1000,"PONTIAC",14140,"O-469") );
        propertyDataTuples.add( Util.buildTuple (1668202,41.04745,-81.484506,"Single Family",24750.0,0.1,"REO","OH","2014-08-22 11:04:50.0","2014-08-19 03:30:00.0",5000,"AKRON",14140,"O-469") );
        propertyDataTuples.add( Util.buildTuple (1668218,42.110872,-87.928582,"Condo",95790.0,0.2,"REO","IL","2014-08-22 09:30:55.0","2014-08-19 03:30:00.0",20000,"WHEELING",14140,"O-469") );
        propertyDataTuples.add( Util.buildTuple (1682923,41.4791176,-73.0479212,"Duplex",79200.0,0.4,"Short Sale","CT","2014-08-26 07:26:50.0","2014-08-23 03:30:00.0",5000,"NAUGATUCK",14452,"O-475") );
        propertyDataTuples.add( Util.buildTuple (1683083,29.114218,-81.008399,"SFR",139050.0,0.6,"REO","FL","2014-08-28 11:12:50.0","2014-08-25 03:30:00.0",30000,"PORT ORANGE",14452,"O-475") );
        propertyDataTuples.add( Util.buildTuple (1684335,42.874283,-78.294144,"SFR",71400.0,.7,"Day 1 REO","NY","2014-08-22 07:08:10.0","2014-08-19 03:30:00.0",20000,"ALEXANDER",14140,"O-469") );
        propertyDataTuples.add( Util.buildTuple (1684861,37.624749,-121.022307,"SFR",134100.0,null,"Short Sale","CA","2014-08-28 14:11:40.0","2014-08-25 06:30:00.0",20000,"MODESTO",14452,"O-475") );
        //ADD THE propertyData TUPLES TO A BAG
        DataBag propertyBag = bagFactory.newDefaultBag();
        for(Tuple t : propertyDataTuples) {
            propertyBag.add(t);
        }
        //ADD THE propertyBag TO EACH USER TUPLE
        for(Tuple t : userDataTuples) {
            t.append(propertyBag);
        }
        inputBagTuple = tupleFactory.newTuple(userDataTuples);
    }



    @Test
    public void testTypeMatchInput() throws Exception {
        inputBagTuple = userDataTuples.get(0);
        EvalFunc<DataBag> fn = new UserAssetTypeMatch();
        DataBag resultBag = fn.exec(inputBagTuple);

        Tuple t;
        Iterator<Tuple> resultTuples = resultBag.iterator();
        t = resultTuples.next();
        //Results come back inside tuples, so we have to cast expectations with that wrapper
        assertEquals("first value in input tuple should be 'SFR,Condo,Townhome'", tupleFactory.newTuple(Arrays.asList("SFR","Condo","Townhome")), inputBagTuple.get(0));
        assertEquals("2nd value in input tuple should be '3'", 3, inputBagTuple.get(1));
    }
    @Test
    public void testTypeMatchOutput() throws Exception {
        inputBagTuple = userDataTuples.get(0);
        EvalFunc<DataBag> fn = new UserAssetTypeMatch();
        DataBag resultBag = fn.exec(inputBagTuple);

        Tuple t;
        Iterator<Tuple> resultTuples = resultBag.iterator();
        t = resultTuples.next();
        //Results come back inside tuples, so we have to cast expectations with that wrapper
        assertEquals("first tuple in output should be 0 penalty", 0.0, t.get(0));
        t = resultTuples.next();
        assertEquals("2nd tuple in output should be 10 penalty", 10.0, t.get(0));
        t = resultTuples.next();
        assertEquals("3rd tuple in output should be 10 penalty", 10.0, t.get(0));
        t = resultTuples.next();
        assertEquals("4th tuple in output should be 10 * 1/4 penalty", 2.5, t.get(0));
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

