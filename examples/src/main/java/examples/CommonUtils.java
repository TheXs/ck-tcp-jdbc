package examples;

import java.util.Random;

/**
 * @ClassName: CommonUtils
 * @Description: TODO
 * @Author: Amitola
 * @Date: 2021/4/30
 **/
public class CommonUtils {
    public static long randomLongGenerator(int decimal) {
        long min = (long) Math.pow(10, decimal);
        long max = min * 10;
        return min + (((long) (new Random().nextDouble() * (max - min))));
    }
}
