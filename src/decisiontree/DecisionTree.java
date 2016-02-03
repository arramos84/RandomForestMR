package decisiontree;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import decisiontree.data.DataSample;
import decisiontree.feature.Feature;
import decisiontree.impurity.EntropyCalculationMethod;
import decisiontree.impurity.ImpurityCalculationMethod;
import decisiontree.label.Label;


/**
 * Decision tree implementation.
 *
 */
public class DecisionTree {
	
	public DecisionTree() {
		
	}

    /** Root node. */
    protected Node root;

    /** Impurity calculation method. */
    protected ImpurityCalculationMethod impurityCalculationMethod = new EntropyCalculationMethod();

    /**
     * When data is considered homogeneous and node becomes leaf and is labeled. If it is equal 1.0 then absolutely all
     * data must be of the same label that node would be considered a leaf.
     */
    protected double homogenityPercentage = 1.0;

    /**
     * Max depth parameter. Growth of the tree is stopped once this depth is reached. Limiting depth of the tree can
     * help with overfitting, however if depth will be set too low tree will not be accurate.
     */
    protected int maxDepth = Integer.MAX_VALUE;
    

    /**
     * Get root.
     */
    public Node getRoot() {
        return root;
    }

    /**
     * Trains tree on training data for provided features.
     * 
     * @param trainingData
     *            List of training data samples.
     * @param features
     *            List of possible features.
     */
    public void train(List<DataSample> trainingData, List<List<Feature>> features) {
        root = growTree(trainingData, features, 1);
    }

    /**
     * Grow tree during training by splitting data recursively on best feature.
     * 
     * @param trainingData
     *            List of training data samples.
     * @param features
     *            List of possible features.
     * 
     * @return Node after split. For a first invocation it returns tree root node.
     */
    protected Node growTree(List<DataSample> trainingData, List<List<Feature>> features, int currentDepth) {

        Label currentNodeLabel = null;
        // if dataset already homogeneous enough (has label assigned) make this node a leaf
        
        if ((currentNodeLabel = getLabel(trainingData)) != null) {
            return Node.newLeafNode(currentNodeLabel);
        }
        
        boolean allEmpty = features.parallelStream().allMatch(p -> p.isEmpty());
        
        boolean stoppingCriteriaReached = allEmpty || currentDepth >= maxDepth;
        if (stoppingCriteriaReached) {
            Label majorityLabel = getMajorityLabel(trainingData);
            return Node.newLeafNode(majorityLabel);
        }
       
        Feature bestSplit = findBestSplitFeature(trainingData, features); // get best set of literals
        
        List<List<DataSample>> splitData = bestSplit.split(trainingData);
        
        // remove best split and irrelevant features from list
        double pVal = bestSplit.getFeatureVal();
        List<List<Feature>> newFeatures = new ArrayList<>();
        
        for(List<Feature> featList: features) {
            newFeatures.add(featList.stream().filter(p -> !p.getColumn().equals(bestSplit.getColumn()) || (p.getFeatureVal() == pVal && !p.equals(bestSplit))).collect(toList()));
            //List<Feature> newFeatList = featList.stream().filter(p -> !p.equals(bestSplit)).collect(toList());
        	//newFeatures.add(newFeatList);
        	//System.out.println(featList.size() + " " +newFeatList.size());
        }
        
        Node node = Node.newNode(bestSplit);
        
        for (List<DataSample> subsetTrainingData : splitData) { // add children to current node according to split
            if (subsetTrainingData.isEmpty()) {
                // if subset data is empty add a leaf with label calculated from initial data
                node.addChild(Node.newLeafNode(getMajorityLabel(trainingData)));
            } else {
                // grow tree further recursively
                node.addChild(growTree(subsetTrainingData, newFeatures, currentDepth + 1));
            }
        }

        return node;
    }

    /**
     * Classify dataSample.
     * 
     * @param dataSample
     *            Data sample
     * @return Return label of class.
     */
    public Label classify(DataSample dataSample) {
        Node node = root;
        while (!node.isLeaf()) { // go through tree until leaf is reached
            // only binary splits for now - has feature first child node(left branch), does not have feature second child node(right branch).
            if (dataSample.has(node.getFeature())) {
                node = node.getChildren().get(0); 
            } else {
                node = node.getChildren().get(1);
            }
        }
        return node.getLabel();
    }

    /**
     * Finds best feature to split on which is the one whose split results in the highest information gain.
     */
    protected Feature findBestSplitFeature(List<DataSample> data, List<List<Feature>> features) {
    	double parentImpurity = impurityCalculationMethod.calculateImpurity(data);
    	double infoGain = -Double.MIN_NORMAL;
    	double childrenImpurity = 1;
    	
        Feature bestSplitFeature = null; // rename split to feature
        
        for(List<Feature> featList: features) {
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

	                if(newGain > infoGain){
	                    infoGain = newGain;
	                	bestSplitFeature = feature;
	                	high = mid - 1;
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
	                    }
	                    else {
	                        high = mid - 1;
	                    }
	            	}
	        	}
        	}
        }
        return bestSplitFeature;
    }

    /**
     * Returns Label if data is homogeneous.
     */
    protected Label getLabel(List<DataSample> data) {
        // group by to map <Label, count>
        Map<Label, Long> labelCount = data.parallelStream().collect(groupingBy(DataSample::getLabel, counting()));
        long totalCount = data.size();
        for (Label label : labelCount.keySet()) {
            long nbOfLabels = labelCount.get(label);
            double labelDistro = (double) nbOfLabels / (double) totalCount;
            if (labelDistro >= homogenityPercentage) {
                return label;
            }
        }
        return null;
    }

    /**
     * Differs from getLabel() that it always return some label and does not look at homogenityPercentage parameter. It
     * is used when tree growth is stopped and everything what is left must be classified so it returns majority label for the data.
     */
    protected Label getMajorityLabel(List<DataSample> data) {
        // group by to map <Label, count> like in getLabels() but return Label with most counts
        return data.parallelStream().collect(groupingBy(DataSample::getLabel, counting())).entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey();
    }

    // -------------------------------- TREE PRINTING ------------------------------------

    public void printTree() {
        printSubtree(root);
    }

    public void printSubtree(Node node) {
        if (!node.getChildren().isEmpty() && node.getChildren().get(0) != null) {
            printTree(node.getChildren().get(0), true, "");
        }
        printNodeValue(node);
        if (node.getChildren().size() > 1 && node.getChildren().get(1) != null) {
            printTree(node.getChildren().get(1), false, "");
        }
    }

    private void printNodeValue(Node node) {
        if (node.isLeaf()) {
            System.out.print(node.getLabel());
        } else {
            System.out.print(node.getName());
        }
        System.out.println();
    }

    private void printTree(Node node, boolean isRight, String indent) {
        if (!node.getChildren().isEmpty() && node.getChildren().get(0) != null) {
            printTree(node.getChildren().get(0), true, indent + (isRight ? "        " : " |      "));
        }
        System.out.print(indent);
        if (isRight) {
            System.out.print(" /");
        } else {
            System.out.print(" \\");
        }
        System.out.print("----- ");
        printNodeValue(node);
        if (node.getChildren().size() > 1 && node.getChildren().get(1) != null) {
            printTree(node.getChildren().get(1), false, indent + (isRight ? " |      " : "        "));
        }
    }

    // For DecisionTree Serialization
    public String JSONTree() throws JsonProcessingException {
    	ObjectMapper JSONMapper = new ObjectMapper();
    	String JSONTree = JSONMapper.writeValueAsString(this);
    	return JSONTree;
    }
}
