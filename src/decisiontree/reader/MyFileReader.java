package decisiontree.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import decisiontree.data.DataSample;
import decisiontree.data.ExchangeDataSample;
import decisiontree.utils.MathUtils;

public class MyFileReader implements Reader {
	
	private static String[] header;
	
	public List<List<DataSample>> readData(String filename, int split) throws IOException {
		List<List<DataSample>> trainAndTest = new ArrayList<>();
		trainAndTest.add(new ArrayList<>());
		trainAndTest.add(new ArrayList<>());
        
 		BufferedReader reader = new BufferedReader(new FileReader(filename));
        header = reader.readLine().split(",");
       
        String strNextLine;
        while ((strNextLine = reader.readLine()) != null) {
           String[] nextLine = strNextLine.split(",");
           if (nextLine != null) {
              int randInt = MathUtils.randomInt(0, 9);
              
              if(randInt >= split) {
        	      trainAndTest.get(0).add(ExchangeDataSample.newExchangeDataSample("Label", header, nextLine));
              }
              else {
            	  trainAndTest.get(1).add(ExchangeDataSample.newExchangeDataSample("Label", header, nextLine));
              }
           }
        }
        reader.close();
        return trainAndTest;
    }

	@Override
	public String[] getHeader() {
		// TODO Auto-generated method stub
		return header;
	}

}