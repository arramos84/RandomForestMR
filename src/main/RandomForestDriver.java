package main;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import decisiontree.data.DataSample;
import decisiontree.data.ExchangeDataSample;
import decisiontree.reader.CassandraReader;
import decisiontree.reader.Reader;
import decisiontree.utils.MathUtils;


public class RandomForestDriver extends Configured implements Tool {

	/** Setup global variables */
	
	public static String[] header;
	protected static Reader reader;

	static final Logger logger = LoggerFactory.getLogger(RandomForestDriver.class);
	private static final String JOB_NAME = "RandomForest";
    static final String HOST = "192.168.56.101";
    private static final String INPUT_COLUMN_FAMILY = "bid_eurusd";
    
    // Main usage: java input output numTrees
	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options
	
		Configuration conf = new Configuration();
        ToolRunner.run(conf, new RandomForestDriver(), args);
        System.exit(0);     
	}
	
	/**
	 * 
	 * @param data
	 *     list of DataSamples
	 * @param fs
	 *     FileSystem being written to (i.e hdfs)
	 * @param filename
	 *     The name of the file being written
	 * @param fileHeader
	 *     String Array of the feature names
	 * @param numLines
	 *     The number of lines per mapper/tree
	 * @param numTrees
	 *     Then number of trees to train
	 * @throws IOException
	 * 
	 * Given a list of DataSamples, randomly write a subset of the data to a file in hdfs
	 * for each tree passed in the command line.
	 * 
	 */
    public static void writeFile(List<DataSample> data, FileSystem fs, String filename, String[] fileHeader, int numLines, int numTrees) throws IOException {
    	Path file = new Path(String.format("hdfs://%s:8020/data/randomforest/input/%s", HOST, filename));
    	
    	if (fs.exists(file)) { 
        	fs.delete(file, true); 
        } 
    	
    	OutputStream os = fs.create(file, new Progressable() {
            public void progress() {
              // TODO: add bytes written??
      	      //logger.info("Writing new hdfs input file...");  
            }
	      });
    	
    	BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8" ));
    	
    	/**
    	for(int i = 0; i < numTrees; i++) {
            for(int j = 0; j < numLines; j++) {
        	    int randInt = MathUtils.randomInt(0, data.size()-1);
        	    DataSample sample = data.get(randInt);
        	    Map<String, Object> values = ((ExchangeDataSample) sample).getValues();
        	
        	    String line = "";
        	    for(String column: header) { 
        		    line += (String)(values.get(column));
       		
         		    if(!column.equalsIgnoreCase("upcount")){
        			    line += ",";
        	 	    }
        	    }
        	    br.write(line);
        	    br.newLine();
            }
    	}**/
    	// TODO: figure out how to speed up random data
        for(int j = 0; j < data.size(); j++) {
        	DataSample sample = data.get(j);
    	    Map<String, Object> values = ((ExchangeDataSample) sample).getValues();
    	
    	    String line = "";
    	    for(String column: header) { 
    		    line += (String)(values.get(column));
   		
     		    if(!column.equalsIgnoreCase("upcount")){
    			    line += ",";
    	 	    }
    	    }
    	    br.write(line);
    	    br.newLine();
        }
	
        br.close();
    }
	
	public int run(String[] args) throws Exception {        
        logger.info("Starting Job: " + JOB_NAME);   
        
        List<List<DataSample>> trainAndTest;
        List<DataSample> trainingData;
        //List<DataSample> testData;
        
        // Read data from Cassandra
        reader = new CassandraReader();
		trainAndTest = reader.readData(INPUT_COLUMN_FAMILY, 2);
		trainingData = trainAndTest.get(0);
    	//testData = trainAndTest.get(1);
		
    	int numLines = trainingData.size();
    	int numTrees = Integer.parseInt(args[2]);
    	int mapLines = numLines/(int)Math.ceil(Double.parseDouble(args[2]));
    	
    	header = reader.getHeader();
    	
    	// Create a filesystem object for writing a file to HDFS on host HOST
    	FileSystem hdfs = FileSystem.get(new URI( String.format("hdfs://%s:8020", HOST)), getConf());
        writeFile(trainingData, hdfs, "training.csv", header, numLines, numTrees);
    	hdfs.close();
    	
    	// Get job instance and set the user's jars to be used first
        Job job = Job.getInstance(getConf(), JOB_NAME);
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        
        job.setJarByClass(RandomForestDriver.class);
        job.setInputFormatClass(NLineInputFormat.class);
        
        NLineInputFormat.addInputPath(job, new Path(args[0]));
        
        // set lines per mapper
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", mapLines);     		                  
        job.setMapperClass(RandomForestMapper.class);
        job.setCombinerClass(RandomForestReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
		job.waitForCompletion(true);
		return 0;
	}
}