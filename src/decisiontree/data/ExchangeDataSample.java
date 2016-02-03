package decisiontree.data;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import decisiontree.label.Label;
import decisiontree.label.DirectionalLabel;


public class ExchangeDataSample implements DataSample {
    
    private Map<String, Object> values = new HashMap<>();
    
    /** Column name which contains data labels. */
    private String labelColumn;
    
    private ExchangeDataSample(String labelColumn, String[] header, String[] dataValues) {
        super();
        this.labelColumn = labelColumn;
        for (int i = 0; i < header.length; i++) {
            this.values.put(header[i].toLowerCase(), dataValues[i]);
        }
    }

    @Override
    public Optional<Object> getValue(String column) {
        return Optional.ofNullable(values.get(column));
    }
    
    @Override
    public Label getLabel() {
    	Label label = null;
    	
        String strLabel = (String)values.get(labelColumn.toLowerCase());
        label = DirectionalLabel.newLabel(strLabel);
        return label;
    }

    /**
     * Create data sample without labels which is used on trained tree.
     */
    public static ExchangeDataSample newClassificationDataSample(String[] header, String[] values) {
        checkArgument(header.length == values.length);
        return new ExchangeDataSample(null, header, values);
    }

    /**
     * @param labelColumn
     * @param header
     * @param values
     * @return
     */
    public static ExchangeDataSample newExchangeDataSample(String labelColumn, String[] header, String[] values) {
        checkArgument(header.length == values.length);
        return new ExchangeDataSample(labelColumn, header, values);
    }
    
    /**
     * 
     * @param argsLenEqual
     */
    public static void checkArgument(boolean argsLenEqual) {
    	if(!argsLenEqual) {
    		throw new IllegalArgumentException("argument length for header and values are not equal!");
    	}
    }
    
    public Map<String, Object> getValues() {
    	return values;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "ExchangeDataSample [values=" + values + "]";
    }
    
    // For DecisionTree Serialization
    public String JSONData() throws JsonProcessingException {
    	ObjectMapper JSONMapper = new ObjectMapper();
    	String JSONData = JSONMapper.writeValueAsString(this);
    	return JSONData;
    }
    
}
