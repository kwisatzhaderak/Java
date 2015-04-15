package com.auction.data.hadoop.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


import java.io.IOException;
import java.util.*;

import static org.apache.pig.PigWarning.UDF_WARNING_1;

/**
 * Created by wrendall on 8/21/2014.
 */
public class NTermEuclideanDistance extends EvalFunc<DataBag> {

    //CONVERT AND VERIFY THAT VALUES ARE THE CORRECT TYPE, IN THIS CASE BAGS
    //CHECK TO SEE THAT WE HAVE SOMETHING TO COMPARE WITH USER PREFERENCES

    @Override
    public DataBag exec(Tuple input) throws IOException {
        int nTerms;
        int nObs;
        int sizeOfInput;
        BagFactory bagFactory = BagFactory.getInstance();
        TupleFactory tupleFactory = TupleFactory.getInstance();
        Tuple tupleFromInput = tupleFactory.newTuple();
        DataBag outputBag = bagFactory.newDefaultBag();
        List<Tuple> tuples = new ArrayList<Tuple>();
        if (input == null || input.size() == 0) {
            return null;
        } else {

            Map<Integer, Tuple> inputMap = new HashMap();
            Map<Integer, Integer> inputSizeMap = new HashMap();

            nTerms = input.size();

            //CONVERT AND VERIFY THAT VALUES ARE THE CORRECT TYPE, IN THIS CASE TUPLES
            for (int n = 0; n < nTerms; n++) {
                Object inputToTest = input.get(n);
                //TEST THE FOLLOWING CASES: INPUT IS NULL, INPUT IS OF SIZE ZERO, INPUT HAS NULLS, INPUT IS ACCEPTABLE.
                //IS THE INPUT A BAG? IF SO CHECK ITS CONTENTS AND ADD THEM, IF PRESENT, TO A TUPLE THEN STORE
                if (inputToTest != null && DataBag.class.isInstance(inputToTest)) {
                    DataBag dataBagInput = (DataBag) inputToTest;
                    tupleFromInput = makeTupleFromBag(dataBagInput, 0.0);
                    if(tupleFromInput != null && tupleFromInput.size() > 0){
                        //SUITABLE TUPLE CONSTRUCTED. STORE IN A MAP
                        sizeOfInput = tupleFromInput.size();
                        inputMap.put(n, tupleFromInput);
                        //STORE THE SIZE TOO, WE'LL USE THIS TO VALIDATE INPUTS
                        inputSizeMap.put(n, sizeOfInput);
                        //IS THE INPUT A TUPLE?  IF SO CHECK ITS CONTENTS AND ADD THEM, IF PRESENT, TO A TUPLE THEN STORE
                    }
                } else {
                    if (inputToTest != null && Tuple.class.isInstance(inputToTest)) {
                        Tuple inputTuple = (Tuple) inputToTest;
                        tupleFromInput = makeTupleFromTuple(inputTuple, 0.0);
                        if (tupleFromInput.size() > 0) {
                            //SUITABLE TUPLE CONSTRUCTED. STORE IN A MAP
                            sizeOfInput = tupleFromInput.size();
                            inputMap.put(n, tupleFromInput);
                            //STORE THE SIZE TOO, WE'LL USE THIS TO VALIDATE INPUTS
                            inputSizeMap.put(n, sizeOfInput);
                        }
                    } else {
                        if (inputToTest != null) {
                            String warningString = "Item " + n + " in argument list not of type tuple or DataBag. It has been excluded";
                            warn(warningString, UDF_WARNING_1);
                        } else {
                            String warningString = "Item " + n + " in argument list is NULL. It has been excluded";
                            warn(warningString, UDF_WARNING_1);
                        }
                    }
                }
            }

            //GET SIZE INFORMATION FOR EVERY INPUT THAT MADE IT INTO THE CALCULATION STEP (THE NON-NULL TUPLES AND BAGS)
            outputBag.clear();

            Object firstKey = inputMap.keySet().toArray()[0];
            nObs = inputMap.get(firstKey).size();
            Integer nTermsPassed = inputMap.size();
            Integer sizeComparisonTotal = 0;
            for (Integer key : inputMap.keySet()) {
                sizeComparisonTotal += inputSizeMap.get(key);
            }
            int expectedTotalSize = nObs * nTermsPassed;
            //VERIFY THAT THE INPUTS ARE ALL THE SAME SIZE AND THEN CALCULATE
            if (expectedTotalSize == sizeComparisonTotal) {
                //LOOP ACROSS EACH OBSERVATION. TAKE THE CORRESPONDING VALUE FROM EACH MAP ENTRY. SQUARE, SUM, SQRT, RETURN.
                for (int j = 0; j < nObs; j++) {
                    double finalDistance = 0.0, totals = 0.0;
                    Tuple outputTuple = TupleFactory.getInstance().newTuple();
                    for (Integer key : inputMap.keySet()) { //looping through bags
                        Object pullJK = inputMap.get(key).get(j);
                        //CHECK IF IT'S NULL, WHICH COULD HAPPEN IF A NULL TUPLE WAS PASSED, THOUGH NOT IF A NULL DATABAG WAS
                        Double distJK = 0.0;
                        if (pullJK != null) {
                            distJK = checkMakeDouble(pullJK, 0.0);
                        } else {
                            distJK = 0.0;
                        }
                        Double squareDist = 0.0;
                        squareDist = distJK * distJK;
                        totals += squareDist;
                    }
                    finalDistance = Math.sqrt(totals);
                    outputTuple.append(finalDistance);
                    outputBag.add(outputTuple);
                }
                return outputBag;
            } else {
                warn("Non-NULL input bags not of the same size. FAILURE!", UDF_WARNING_1);
                return null;
            }
        }
    }

    //METHODS INTRODUCED IN CLASS
    //Make a value a double
    private Double checkMakeDouble(Object numericValue, Double defaultValue) {
        if (numericValue != null && Double.class.isInstance(numericValue)) {
            return (Double) numericValue;
        }
        else {
            if (numericValue != null && Float.class.isInstance(numericValue)) {
                return Double.valueOf((Float) numericValue);
            }
            else {
                if (numericValue != null && Integer.class.isInstance(numericValue)) {
                return Double.valueOf((Integer) numericValue);
                }
                else {
                    return defaultValue;
                }
            }
        }
    }
    //Unpack a tuple of values and repack it into a Tuple of Doubles, converting non-double-ables to 0.0
    private Tuple makeTupleFromTuple(Tuple collection, Double defaultValue) {
        Tuple madeTuple = TupleFactory.getInstance().newTuple();
        if (collection != null && Tuple.class.isInstance(collection)) {
            if (collection.size() > 0) {
                int sizeOfTuple = collection.size();
                for (int i = 0; i < sizeOfTuple; i++) {
                    Object thisObject = null;
                    try {
                        thisObject = collection.get(i);
                    }
                    catch (ExecException e) {
                        warn("Tuple in input contained null Object. Converted to default, almost certainly 0.0", UDF_WARNING_1);
                        thisObject = defaultValue;
                    }
                    if (thisObject != null && Tuple.class.isInstance(thisObject)) {
                        Tuple innerTuple = (Tuple) thisObject;
                        if (innerTuple.size() == 1) {
                            Object thisValue = defaultValue;
                            try {
                                thisValue = innerTuple.get(0);
                            }
                            catch (ExecException e) {
                                warn("Tuple in input Tuple contained null Object. Converted to default value, probably 0.0", UDF_WARNING_1);
                                thisValue = defaultValue;
                            }
                            Double thisDouble = checkMakeDouble(thisValue, defaultValue);
                            madeTuple.append(thisDouble);
                        } else {
                            warn("Tuple in input Tuple did not contain exactly 1 value, and was replaced with default value, likely 0.0", UDF_WARNING_1);
                            madeTuple.append(defaultValue);
                        }
                    } else {
                        if (thisObject != null && Double.class.isAssignableFrom(thisObject.getClass())) {
                            Double thisDouble = checkMakeDouble(thisObject, defaultValue);
                            madeTuple.append(thisDouble);
                        } else {
                            warn("Value in input Tuple cannot be changed to a Double. It was replaced with default value, typically 0.0", UDF_WARNING_1);
                            madeTuple.append(defaultValue);
                        }
                    }
                }
            } else {
                warn("Input Tuple contained no values. No unpacking or value verification performed.", UDF_WARNING_1);
            }
        } else {
            warn("Expected non-null tuple input, but a non-Tuple was given", UDF_WARNING_1);
        }
        return madeTuple;
    }

    //Unpack a DataBag of Tuples/Values and repack it into a Tuple of Doubles, converting non-double-ables to 0.0
    private Tuple makeTupleFromBag(DataBag collection, Double defaultValue) {
        Tuple madeTuple = TupleFactory.getInstance().newTuple();
        if (collection != null && DataBag.class.isInstance(collection)) {
            if (collection.size() > 0) {
                Iterator<Tuple> collectionIterator = collection.iterator();
                while (collectionIterator.hasNext()) {
                    Tuple thisTuple = collectionIterator.next();
                    Object thisObject = null;
                    if ( thisTuple.size() == 1) {
                        try {
                            thisObject = thisTuple.get(0);
                        } catch (ExecException e) {
                            warn("Tuple in input Databag contained null Object. Converted to default. Almost certainly 0.0", UDF_WARNING_1);
                            thisObject = defaultValue;
                        }
                    }
                    else {
                        warn("Tuple in input DataBag did not contain only 1 value. Converted to default; usually 0.0", UDF_WARNING_1);
                        thisObject = defaultValue;
                    }
                    if (thisObject != null && Double.class.isAssignableFrom(thisObject.getClass())) {
                        Double thisDouble = checkMakeDouble(thisObject, defaultValue);
                        madeTuple.append(thisDouble);
                    } else {
                        warn("Value in input Tuple from input DataBag cannot be changed to a Double. It was replaced with default value, typically 0.0", UDF_WARNING_1);
                        madeTuple.append(defaultValue);
                    }
                }
            }
            else {
                warn("Input DataBag contained no values. No unpacking or value verification performed.", UDF_WARNING_1);
            }
        } else {
            warn("Expected non-null DataBag input, but a non-DataBag was given", UDF_WARNING_1);
        }
        return madeTuple;
    }
}
