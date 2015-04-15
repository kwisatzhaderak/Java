package com.auction.data.hadoop.pig.udf;

import com.auction.data.hadoop.pig.udf.util.StringUtils;
import com.auction.data.utils.StringUtil;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.tools.ant.taskdefs.condition.IsFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wrendall on 8/18/2014.
 */
public class UserAssetTypeMatch extends EvalFunc<DataBag> {

    //CREATE A SINGLE OUTPUT BAG TO RETURN TO PIG
    DataBag outputBag = BagFactory.getInstance().newDefaultBag();

    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 4) return null;

        //PARSE THE INPUT FOR CONTROL AND COMPARISON INFORMATION
        Object arg1 = input.get(0); //Tuple of preferredAssetClassesOfUser strings //could be null
        Object arg2 = input.get(1); //User-Supplied Parameter: Location of Comparison Value in Property Set
        Object arg3 = input.get(2); //Optional string of additional match parameters supplied by user
        //NOTE: ARG 3 IS CURRENTLY RESERVED FOR MAX PENALTY APPLIED TO NON-MATCH PROPERTIES GIVEN A USER'S TYPE PREFERENCE
        //DEFAULT VALUE IS 1, MEANING NON-MATCHES APPEAR 1 MILE FURTHER THAN REAL MATCHES
        Object arg4 = input.get(3); //Bag of tuples to generate distances for

        //INSTANTIATE PLACEHOLDER VALUES
        int comparisonPosition;
        List<String> preferredAssetClassesOfUser;
        Double nonMatchPenalty = 1.0;
        DataBag bagToCompare;

        //CONVERT AND VERIFY THAT VALUES ARE THE CORRECT TYPE
        if (arg1 != null && arg1 instanceof Tuple) {
            Tuple assetClassesTuple = (Tuple) arg1;
            preferredAssetClassesOfUser = new ArrayList<String>(assetClassesTuple.size());
            for( int i = 0; i < assetClassesTuple.size(); i++ ) {
                Object thisValue = assetClassesTuple.get(i);
                if( thisValue != null && String.class.isInstance(thisValue) ) {
                    String thisAssetClass = (String) thisValue;
                    preferredAssetClassesOfUser.add(thisAssetClass);
                }
                else
                    warn("preferred asset classes tuple contains non-string value!", PigWarning.UDF_WARNING_3);
            }
        } else {
            //NOT TUPLE? NULL FOR THIS OPERATION AND EXIT, WE WILL HANDLE NULL VALUES IN THE FINAL DISTANCE CALCULATION
            warn("null tuple of preferred asset classes passed - returning null output, which is surely not what you wanted!", PigWarning.UDF_WARNING_1);
            return null;
        }

        if (arg2 != null && Integer.class.isInstance(arg2)) {
            comparisonPosition = (Integer)arg2;
        } else {
            //NOT INT? NULL FOR THIS OPERATION AND EXIT, WE WILL HANDLE NULL VALUES IN THE FINAL DISTANCE CALCULATION
            warn("null column position for property type passed - returning null output, which is surely not what you wanted!", PigWarning.UDF_WARNING_1);
            return null;
        }

        //CHECK FOR A USER DEFINED MAX PENALTY, BUT IF IT'S NOT SUPPLIED, JUST LEAVE IT AS 1.0
        if (arg3 != null ) {
            if(Float.class.isInstance(arg3)) {
                nonMatchPenalty = Double.valueOf((Float) arg3);
            } else {
                if (Double.class.isInstance(arg3)) {
                    nonMatchPenalty = (Double) arg3;
                }
            }
        }
        if (arg4 != null && DataBag.class.isInstance(arg4)) {
            bagToCompare = (DataBag) arg4;
        } else {
            warn("null bag of assets passed - returning null output, which is surely not what you wanted!", PigWarning.UDF_WARNING_1);
            return null;
        }

        //REMOVE ANY PREVIOUS VALUES THAT HAVE PERSISTED OVER ITERATIONS. THIS CAN HAPPEN ON FAILURES
        outputBag.clear();

        for (Tuple thisTuple : bagToCompare) {
            Object rawAssetType = thisTuple.get(comparisonPosition);
            if (rawAssetType != null && String.class.isInstance(rawAssetType)) {
                String thisAssetType = (String) rawAssetType;
                Double distance = getPreferredCriteria(thisAssetType, preferredAssetClassesOfUser, nonMatchPenalty);
                Tuple outputTuple = TupleFactory.getInstance().newTuple(1);
                outputTuple.set(0, distance);
                outputBag.add(outputTuple);
            } else {
                return null;
            }
        }
        return outputBag;
    }

    //METHODS INTRODUCED IN CLASS
    //ASSET PREFERENCE
    //-----UNPACK THE TUPLE OF ASSET TYPES INTO A LIST OF STRINGS
    //-----COMPARE STRINGS, IF THERE IS A MATCH, THEN DISTANCE = 0, IF NOT THEN 30 FOR RESI, 300 FOR COMMERCIAL
    private Double getPreferredCriteria(String assetTypeOfProperty, List<String> preferredAssetClasses, Double nonMatchPenalty) {
        double sizeOfPreference = preferredAssetClasses.size();
        double prefCriteria = nonMatchPenalty;
        if (StringUtils.isEmpty(assetTypeOfProperty) || preferredAssetClasses == null || preferredAssetClasses.size() == 0) {
            prefCriteria = nonMatchPenalty;
        }
        else {
            if (preferredAssetClasses.contains(assetTypeOfProperty)) {
                for (int n = 0; n < sizeOfPreference; n++) {
                    String thisPriorityAssetPref = preferredAssetClasses.get(n);
                    if (thisPriorityAssetPref.equals(assetTypeOfProperty)) {
                        double numerator = (double)n;
                        double denominator = sizeOfPreference - 1.0;
                        return Math.pow((numerator / denominator),2) * nonMatchPenalty;
                    }
                }
            }
            else {
                prefCriteria = nonMatchPenalty;
            }
        }
        return prefCriteria;
    }
}
