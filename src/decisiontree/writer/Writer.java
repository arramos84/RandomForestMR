package decisiontree.writer;

import java.io.IOException;
import java.util.List;

import decisiontree.DecisionTree;
import decisiontree.data.DataSample;

public interface Writer {
    void writeData(String target, List<DecisionTree> trees, List<DataSample> data) throws IOException;
}
