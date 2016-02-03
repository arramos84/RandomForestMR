package decisiontree.feature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Predicate;

import main.Serializer;
import decisiontree.data.DataSample;

/**
 * Feature type that splits data into 2 sublists - one that 
 * 
 * @param <T> Feature data type (string, number)
 */
public class PredicateFeature implements Feature {
    
    /** Data column used by feature. */
    private String column; // TODO multiple columns per feature

    /** Predicate used for splitting. */
    private SerializableDoublePredicate predicate;
    
    /** Feature Label used for visualization and testing the tree. */
    private String label;
    
    /** Value for the predicate if applicable. */
    private double featureValue;

    /**
     * Constructor.
     * 
     * @param column Column in data table.
     * @param predicate Predicate used for splitting. For example if value is equal to some value, or is more/less.
     * @param featureValue Feature value used by predicate when comparing with data.
     */
    private PredicateFeature(String column, SerializableDoublePredicate predicate, String label, double featureVal) {
        super();
        this.column = column;
        this.predicate = predicate;
        this.label = label;
        this.featureValue = featureVal;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean belongsTo(DataSample dataSample) { // TODO implement other splits (in different type of feature)   	
        Optional<Object> optionalValue = dataSample.getValue(column);
        Double optionalDouble = Double.parseDouble(optionalValue.get().toString());
        boolean b = optionalValue.isPresent() ? predicate.test(optionalDouble) : false;
        return b;
    }

    @Override
    public String toString() {
        return label;
    }


    /**
     * Static factory method to create a new feature. This one accepts any predicate.
     * 
     * @param column Column to use in data.
     * @param predicate Predicate to use for splitting.
     * @return New feature.
     */
    public static Feature newFeature(String column, SerializableDoublePredicate predicate, String predicateString, double featVal) {
        return new PredicateFeature(column, predicate, String.format("%s %s", column, predicateString), featVal);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((column == null) ? 0 : column.hashCode());
        result = prime * result + ((label == null) ? 0 : label.hashCode());
        result = prime * result + ((predicate == null) ? 0 : predicate.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        @SuppressWarnings("rawtypes")
        PredicateFeature other = (PredicateFeature) obj;
        if (column == null) {
            if (other.column != null)
                return false;
        } else if (!column.equals(other.column))
            return false;
        if (label == null) {
            if (other.label != null)
                return false;
        } else if (!label.equals(other.label))
            return false;
        if (predicate == null) {
            if (other.predicate != null)
                return false;
        } else if (!predicate.equals(other.predicate))
            return false;
        return true;
    }
    
   public String getColumn() {
    	return column;
    } 
   
   public double getFeatureVal() {
	   return featureValue;
   }
   
   @Override
   public void readFields(DataInput in) throws IOException {

   }

   @Override
   public void write(DataOutput out) throws IOException {
	   byte[] bytes = Serializer.serialize(predicate);
	   out.write(bytes);
       out.writeBytes(column);
       out.writeBytes(label);
       out.writeDouble(featureValue);
   }

@Override
public int compareTo(Object o) {
	// TODO Auto-generated method stub
	return 0;
}
}
