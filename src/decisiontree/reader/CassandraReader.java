package decisiontree.reader;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ColumnDefinitions.Definition;

import decisiontree.data.DataSample;
import decisiontree.data.ExchangeDataSample;
import decisiontree.utils.MathUtils;

public class CassandraReader implements Reader {

	private static Cluster cluster;
	private static Session session;
	private static String[] header;
	private static String[] types;
	
	@Override
	public List<List<DataSample>> readData(String table, int split) {
		cluster = Cluster.builder().addContactPoint("192.168.56.101").build();
		session = cluster.connect("exchange");
		
		ResultSet results = session.execute("SELECT * FROM " + table);
		//ResultSet results = session.execute("select * from bid_eurusd where year=2009 and datetime < '2009-05-07 03:00:00+0000'");
		
		ColumnDefinitions cDefs = results.getColumnDefinitions();
		header = new String[cDefs.size()-2];
		types = new String[cDefs.size()-2];
		
		int i = 0;
		for(Definition def: cDefs) {
			String name = def.getName();
			DataType type = def.getType();
			if(!name.equalsIgnoreCase("year") && !name.equalsIgnoreCase("datetime")){
				header[i] = name.toLowerCase();
				types[i] = type.toString();
				i++;
			}
		}
	
		List<List<DataSample>> trainAndTest = new ArrayList<>();
		trainAndTest.add(new ArrayList<>());
		trainAndTest.add(new ArrayList<>());
		for (Row row : results) {
			String[] values = new String[header.length];
			
			for(int j = 0; j < header.length; j++){
				String val = getValueByType(row, types[j], j+2);
				values[j] = val;
			}
			
			int randInt = MathUtils.randomInt(0, 9);
            
            if(randInt >= split) {
      	      trainAndTest.get(0).add(ExchangeDataSample.newExchangeDataSample("Label", header, values));
            }
            else {
          	  trainAndTest.get(1).add(ExchangeDataSample.newExchangeDataSample("Label", header, values));
            }
		}
		
		cluster.close();
		
		return trainAndTest;
	}
	
	public static String getValueByType(Row row, String type, int index) {
		String value = null;
		switch(type) {
		    case "float":
		    	value = String.valueOf(row.getFloat(index));
		    	break;
		    case "int":
		    	value = String.valueOf(row.getInt(index));
		    	break;
		    default:
		    	break;
		}
		return value;
	}

	@Override
	public String[] getHeader() {
		// TODO Auto-generated method stub
		return header;
	}

}