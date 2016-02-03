package decisiontree.feature;

import java.io.Serializable;
import java.util.function.DoublePredicate;

@FunctionalInterface
public interface SerializableDoublePredicate extends DoublePredicate, Serializable {

}
