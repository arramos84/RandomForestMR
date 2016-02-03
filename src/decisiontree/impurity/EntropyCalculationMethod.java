package decisiontree.impurity;

import static decisiontree.utils.MathUtils.log2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import decisiontree.data.DataSample;
import decisiontree.label.Label;

/**
 * Entropy calculator. -p log2 p - (1 - p)log2(1 - p) - this is the expected information, in bits, conveyed by somebody
 * telling you the class of a randomly drawn example; the purer the set of examples, the more predictable this message
 * becomes and the smaller the expected information.
 * 
 *
 */
public class EntropyCalculationMethod implements ImpurityCalculationMethod {

    /**
     * {@inheritDoc}
     */
    @Override
    public double calculateImpurity(List<DataSample> splitData) {
        List<Label> labels = splitData.parallelStream().map(data -> data.getLabel()).distinct().collect(Collectors.toList());
        if (labels.size() > 1) {
            double p = getEmpiricalProbability(splitData, labels.get(0), labels.get(1)); // TODO fix to multiple labels
            double entropy = -1.0 * p * log2(p) - ((1.0 - p) * log2(1.0 - p));
            return entropy;
        } else if (labels.size() == 1) {
            return 0.0; // if only one label data is pure
        } else {
            throw new IllegalStateException("This should never happen. Probably a bug.");
        }
    }
}
