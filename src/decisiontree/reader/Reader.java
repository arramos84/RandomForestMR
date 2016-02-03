package decisiontree.reader;

import java.io.IOException;
import java.util.List;

import decisiontree.data.DataSample;

public interface Reader {
	
	List<List<DataSample>> readData(String target, int split) throws IOException;
	
	String[] getHeader();
}
