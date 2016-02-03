package decisiontree.writer;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import decisiontree.DecisionTree;
import decisiontree.data.DataSample;
import decisiontree.utils.MathUtils;

public class CassandraWriter implements Writer {

	private static Cluster cluster;
	private static Session session;
	
	@Override
	public void writeData(String table, List<DecisionTree> trees, List<DataSample> data) {
		int numCorrect = 0;
		int numWrong = 0;
		
		for(DataSample dataSample: data) {
            String actualVal = dataSample.getLabel().getPrintValue();
            String finalPredVal;
            	
            int upCount = 0;
           	int downCount = 0;
           	for(DecisionTree randTree: trees){
           		String predVal = randTree.classify(dataSample).getPrintValue();
           		
           		if(predVal.equalsIgnoreCase("up")){
           			upCount++;
           		}
           		else if(predVal.equalsIgnoreCase("down")) {
           			downCount++;
           		}
           		else {
           			System.out.println("Unexpected Label: " + predVal);
            	}
            }
            	
            if(upCount == downCount){
           		int randInt = MathUtils.randomInt(0, 1);
           		if(randInt == 1){
           			finalPredVal = "up";
           		}
           		else {
           			finalPredVal = "down";
           		}
           	}
           	else if(upCount > downCount){
           		finalPredVal = "up";
           	}
           	else {
           		finalPredVal = "down";
            }
            	
           	if(actualVal.equalsIgnoreCase(finalPredVal)){
                numCorrect++;
            }
           	else {
                numWrong++;
            }
        }
       
        // print out the correct predictions and the accuracy
        int totalPredictions = numCorrect + numWrong;
        DecimalFormat twoDigit = new DecimalFormat("#,###.00");
        String accuracy = (twoDigit.format(100*(double)(numCorrect)/(double)(totalPredictions)));
		
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect("exchange");
	    
		LocalDateTime date = LocalDateTime.now();
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String dateStr = date.format(fmt);
		int year = Integer.parseInt(dateStr.split("-")[0]);
		
		String output = String.format("%s,'%sZ',%s,%s,%s,%s", year, dateStr,  accuracy, numCorrect, trees.size(), totalPredictions);
		String insertCommand = String.format("INSERT INTO %s (year, datetime, accuracy, correct, num_trees, total) " +
		                       "VALUES (%s);", table, output);
		
		System.out.println(String.format("Accuracy = %s", accuracy));
		session.execute(insertCommand);
		cluster.close();
	}

}