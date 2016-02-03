package decisiontree.feature;

import static java.util.stream.Collectors.partitioningBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.WritableComparable;

import decisiontree.data.DataSample;


@SuppressWarnings("rawtypes")
public interface Feature extends WritableComparable {
    
    /**
     * Calculates and checks if data contains feature.
     * 
     * @param dataSample Data sample.
     * @return true if data has this feature and false otherwise.
     */
    boolean belongsTo(DataSample dataSample);
    
    /**
     * 
     * @return a string, name of the column the feature belongs to
     */
    String getColumn();
    
    /**
     * 
     * @return
     */
    double getFeatureVal();

    /**
     * Split data according to if it has this feature.
     * 
     * @param data Data to by split by this feature.
     * @return Sublists of split data samples.
     */
    default List<List<DataSample>> split(List<DataSample> data) {
        List<List<DataSample>> result = new ArrayList<>();
        Map<Boolean, List<DataSample>> split = data.parallelStream().collect(partitioningBy(dataSample -> belongsTo(dataSample)));
        
        if (split.get(true).size() > 0) {
            result.add(split.get(true));
        } else {
            result.add(new ArrayList<>());
        }
        if (split.get(false).size() > 0) {
            result.add(split.get(false));
        } else {
            result.add(new ArrayList<>());
        }
        return result;
    }
}
