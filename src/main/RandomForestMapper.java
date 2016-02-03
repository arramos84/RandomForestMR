package main;

import static decisiontree.feature.PredicateFeature.newFeature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import randomforest.RandomTree;
import decisiontree.DecisionTree;
import decisiontree.data.DataSample;
import decisiontree.data.ExchangeDataSample;
import decisiontree.feature.Feature;
import decisiontree.feature.SerializableDoublePredicate;
import decisiontree.utils.MathUtils;

public class RandomForestMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static List<DataSample> samples = new ArrayList<>();
	private static HashMap<String, String> featureTypes = new HashMap<String, String>();
    private static String[] header = RandomForestDriver.header;
    int numFeatures = (int) Math.sqrt((double)header.length);
    
    
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		  
          StringTokenizer itr = new StringTokenizer(ivalue.toString());
	      while (itr.hasMoreTokens()) {
	    	String[] tokens = itr.nextToken().split(",");
	    	
	    	if (tokens != null) {
	            samples.add(ExchangeDataSample.newExchangeDataSample("Label", header, tokens));
	    	}
	      }
	      
	      initFeatureTypes();
	}
	
	protected static void initFeatureTypes() {
    	featureTypes.put("day", "discrete");
    	featureTypes.put("hour", "discrete");
    	featureTypes.put("high", "real");
    	featureTypes.put("close", "real");
    	featureTypes.put("spread", "real");
    	featureTypes.put("momentumday", "real");
    	featureTypes.put("momentumhour", "real");
    	featureTypes.put("rocday", "real");
    	featureTypes.put("rochour", "real");
    	featureTypes.put("upcount", "discrete");
    }
	
	 /**
     * 
     * @param data 
     *     List<DataSample> all the sample training data
     * @return
     *     a List of sorted and unique Features
     */
    protected static List<List<Feature>> getFeatures(List<DataSample> data) {   	
    	List<List<Feature>> features = new ArrayList<>();
    	Map<String, List<Double>> featureValues = new HashMap<>();
    	
    	for(int i = 0; i < header.length; i++) {
    		String column = header[i].toLowerCase();

    		if(featureTypes.get(column) != null){
    		    featureValues.put(column, new ArrayList<Double>());
    		    features.add(new ArrayList<Feature>());
    		}
    	}
    
    	for(DataSample sample: data) {
    		for(Map.Entry<String, Object> entry: ((ExchangeDataSample) sample).getValues().entrySet()){
    			String key = entry.getKey().toLowerCase();
    			Object val = entry.getValue();
    			
    			if(featureTypes.get(key) != null){
    				double value = Double.parseDouble((String)val);
    			    featureValues.get(key).add(value);
    			}
    		}
    	}
    	
    	for(String key: featureValues.keySet()) {
    		List<Double> sorted = new ArrayList<Double>(new LinkedHashSet<Double>(featureValues.get(key)));
    		Collections.sort(sorted);
    		
    		List<Feature> newFeats = new ArrayList<>();
    		
    		switch(featureTypes.get(key)){
    		    case "discrete":
    		    	for(Double val: sorted) {
    		    		SerializableDoublePredicate featPred = (SerializableDoublePredicate) p -> p < val;
    	    			newFeats.add(newFeature(key, featPred, "< " + val, val));
    		    	}
    		        break;
    		    case "real":
    		    	for(Double val: sorted) {
    		    		SerializableDoublePredicate featPred = (SerializableDoublePredicate) p -> p >= val;
    	    			newFeats.add(newFeature(key, featPred, ">= " + val, val));
    	    		}
    		    	break;
    		    default:
    		    	System.err.println("Unknown case for column " +  key);
    		    	System.exit(1);
    		    	break;
    		}
    		
            features.add(newFeats);
    	}
    	
        return features;
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	int sampleSize = samples.size();
    	
    	List<DataSample> trainData = new ArrayList<>();
        
        for(int i = 0; i < sampleSize; i++) {
  	      int randInt = MathUtils.randomInt(0, sampleSize);
  	      DataSample ds = samples.get(randInt);
  	      trainData.add(ds);
        }
    
        DecisionTree tree = new RandomTree(numFeatures);
        List<List<Feature>> features = getFeatures(trainData);
        tree.train(trainData, features);
    	
        context.write(new Text("Tree"), new Text(tree.JSONTree()));
    }
}

