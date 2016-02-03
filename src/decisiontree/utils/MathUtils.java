package decisiontree.utils;

import java.util.Random;


public class MathUtils {

    public static double log2(double x) {
        return Math.log(x) / Math.log(2);
    }
    
    public static int randomInt(int min, int max){
    	Random random = new Random();
    	return random.nextInt(max) + min;
    }
}
