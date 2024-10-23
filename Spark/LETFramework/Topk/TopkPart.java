package LETFramework.Topk;

import common.BOBHash32;
import javafx.util.Pair;

import java.util.Map;
import java.util.Random;

public abstract class TopkPart {
    protected BOBHash32 hsh;

    public void initial() {
        Random rnd = new Random();
        hsh = new BOBHash32(rnd.nextInt(BOBHash32.MAX_PRIME32));
    }

    public void initial(TopkPart refer) {
        hsh = new BOBHash32(refer.hsh.prime32Num);
    }


    public abstract Pair<Integer, Pair<Integer, Integer>> insert(Integer key, Integer val);

    public abstract Map<Integer, Integer> queryAll();

    public abstract Integer query(Integer key);

    public static TopkPart create(Integer type, Integer memory_in_byte) {
        switch (type) {
            case 2:
                return new SpaceSavingBased(memory_in_byte);
            case 1:
                return new HeavyGuardianBased(memory_in_byte);
            case 3:
                return new FrequentBased(memory_in_byte);
            case 0: default:
                return new ElasticSketchBased(memory_in_byte);
        }
    }
    public static int getCounterVal(int val) {
        return ((val) & 0x7FFFFFFF);
    }

    public static int updateGuardVal(int guardVal) {
        return guardVal+1;
    }
}
