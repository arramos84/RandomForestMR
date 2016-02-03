package randomforest;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import decisiontree.DecisionTree;
import decisiontree.data.DataSample;
import decisiontree.data.ExchangeDataSample;
import decisiontree.feature.Feature;
import decisiontree.utils.MathUtils;

public class RandomTree extends DecisionTree {
    
	private int numFeatures;
	
	public RandomTree() {
		// for <init>() call in MapReduce
	}
	
	/**
	 * Constructor
	 * @param numFeatures
	 *     the number of features in the data
	 */
	public RandomTree(int numFeatures) {
		super();
		this.numFeatures = numFeatures;
	}
	
	@Override
    protected Feature findBestSplitFeature(List<DataSample> data, List<List<Feature>> features) {
		double parentImpurity = impurityCalculationMethod.calculateImpurity(data);
    	double infoGain = -Double.MIN_NORMAL;
    	double childrenImpurity = 1;
    	
        Feature bestSplitFeature = null; // rename split to feature
		
		while(bestSplitFeature == null) {
		    /** start with the highest (worst) impurity. */
            //double currentImpurity = 1;

            int randFeatNum = MathUtils.randomInt(1, numFeatures);
        
            // Map of integers to feature names for selecting random features
            Map<Integer, String> featMap = new HashMap<Integer, String>();
    	    ExchangeDataSample singleSample = (ExchangeDataSample)data.get(0);
    	
    	    // populate map where key=number val=feature name
    	    int i = 1;
    	    for(Map.Entry<String, Object> entry: ((ExchangeDataSample) singleSample).getValues().entrySet()) {
    		    String key = entry.getKey();
    		    featMap.put(i, key);
    		    i++;
    	    }
    	
    	    // initialize set of feature names
    	    Set<String> featSet = new HashSet<String>();
    	    // randomly populate set with randFeatNum names
    	    while(featSet.size() < randFeatNum) {
    		    int rand = MathUtils.randomInt(1, featMap.size());
    		    featSet.add(featMap.get(rand));
    	    }

            for(List<Feature> featList: features) {
            	if(!featList.isEmpty() && featSet.contains(featList.get(0).getColumn())) {
	            	int listSize = featList.size();
	            	int low = 0;
	            	int high = listSize - 1;
	            	int mid = low + (high - low) / 2;
	            	
	            	if(listSize > 0){
	    	        	Feature firstFeature = featList.get(mid);
	    	        	while(low <= high){
	    	        		mid = low + (high - low) / 2;
	    	        		Feature feature = featList.get(mid);
	    	        		
	    	        		List<List<DataSample>> splitData = feature.split(data);
	    	        		// totalSplitImpurity = sum(singleLeafImpurities) / nbOfLeafs
	    	                // in other words splitImpurity is average of leaf impurities
	    	                childrenImpurity = splitData.parallelStream().filter(list -> !list.isEmpty()).mapToDouble(list -> impurityCalculationMethod.calculateImpurity(list)).average().getAsDouble();
	    	                double newGain = parentImpurity - childrenImpurity;
	    	                //System.out.println(infoGain + " " + newGain);
	    	                if(newGain > infoGain){
	    	                    infoGain = newGain;
	    	                	bestSplitFeature = feature;
	    	                	high = mid - 1;
	    	                	//System.out.println(infoGain + " " + feature);
	    	                }
	    	                else {
	    	                	low = mid + 1;
	    	                }
	    	        	}
	    	        	if(bestSplitFeature == null) {
	    	        		bestSplitFeature = firstFeature;
	    	        	}
	    	        	if(bestSplitFeature.getFeatureVal() == firstFeature.getFeatureVal()) {
	    	        		low = 0;
	    	        		high = listSize - 1;
	    	        		while(low <= high){
	    	            		mid = low + (high - low) / 2;
	    	            		Feature feature = featList.get(mid);
	    	            		
	    	            		List<List<DataSample>> splitData = feature.split(data);
	    	            		// totalSplitImpurity = sum(singleLeafImpurities) / nbOfLeafs
	    	                    // in other words splitImpurity is average of leaf impurities
	    	                    childrenImpurity = splitData.parallelStream().filter(list -> !list.isEmpty()).mapToDouble(list -> impurityCalculationMethod.calculateImpurity(list)).average().getAsDouble();
	    	                    double newGain = parentImpurity - childrenImpurity;
	    	                    
	    	                    if(newGain > infoGain){
	    	                        infoGain = newGain;
	    	                    	bestSplitFeature = feature;
	    	                    	low = mid + 1;
	    	                    	//System.out.println(infoGain + " " + feature);
	    	                    }
	    	                    else {
	    	                        high = mid - 1;
	    	                    }
	    	            	}
	    	        	}
	            	}
            	}
            }
		}
        return bestSplitFeature;
    }
	
	public int getNumFeatures() {
		return numFeatures;
	}
}
