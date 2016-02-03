package decisiontree.label;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Directional label, labels up/down as true/false.
 *
 *
 */
public class DirectionalLabel extends Label {
    
    public static final Label UP_LABEL = DirectionalLabel.newLabel(true);

    public static final Label DOWN_LABEL = DirectionalLabel.newLabel(false);
    
    /** Label. */
    private boolean label;
    
    /**
     * Constructor.
     */
    public DirectionalLabel(boolean label) {
        super();
        this.label = label;
    }
    
    /**
     * Constructor.
     */
    public DirectionalLabel(String labelDirection) {
        super();
        boolean label;
        if(labelDirection.equalsIgnoreCase("0")){
        	label = false;
        }
        else {
        	label = true;
        }
        this.label = label;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getPrintValue() {
        return label ? "Up" : "Down";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return String.valueOf(label);
    }
    
    /**
     * Static factory method.
     */
    public static Label newLabel(Boolean label) {
        return new DirectionalLabel(label);
    }

    /**
     * Static factory method.
     */
    public static Label newLabel(String label) {
        return new DirectionalLabel(label);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (label ? 1231 : 1237);
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
        DirectionalLabel other = (DirectionalLabel) obj;
        if (label != other.label)
            return false;
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
    	String labelDirection = null;
    	if(label){
    		labelDirection = "Up";
    	}
    	else {
    		labelDirection = "Down";
    	}
        return "DirectionalLabel [label=" + labelDirection + "]";
    }
}
