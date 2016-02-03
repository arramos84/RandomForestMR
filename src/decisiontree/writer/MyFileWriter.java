package decisiontree.writer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import decisiontree.DecisionTree;
import decisiontree.data.DataSample;
import decisiontree.utils.MathUtils;

public class MyFileWriter implements Writer {
	
    public void writeData(String filename, List<DecisionTree> trees, List<DataSample> data) throws IOException {       
    	FileWriter writer = new FileWriter(new File(filename));
        writer.append("time,numtrees,correct,total,accuracy").append("\n");
        
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
        String accuracy = (twoDigit.format(100*(double)(numCorrect)/(double)(totalPredictions))+ "%");
        
        writer.append(String.format("%s,%s,%s,%s,%s", LocalDateTime.now(), trees.size(), numCorrect, totalPredictions, accuracy)).append("\n");
        System.out.println(String.format("Accuracy = %s", accuracy));
        writer.flush();
        writer.close();
    }
}
