package LETFramework.Topk;

import common.utils;
import javafx.util.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import common.BOBHash32;

public class FrequentBased extends TopkPart implements Serializable {
    private int bucketNum;
    private int[] nCounters;
    private BOBHash32 hsh;
    private Bucket[] buckets;
    public FrequentBased(int memInBytes) {
        bucketNum = memInBytes/Bucket.SIZE;
        buckets = new Bucket[bucketNum];
        nCounters = new int[bucketNum];
        hsh = new BOBHash32();
        for (int i = 0; i < bucketNum; i++) {
            buckets[i]=new Bucket();
        }
    }
    public static boolean judgeIfSwap(int minVal, int guardVal) {
        return guardVal > (minVal);
    }
    @Override
    public Pair<Integer, Pair<Integer, Integer>> insert(Integer key, Integer val) {
        int pos = (int)(hsh.run(utils.intToByteArray(key)) % bucketNum);
        int minCounterVal;
        int minCounter;

        do {
            int matched = -1, empty = -1;
            minCounter = 0;
            minCounterVal = getCounterVal(buckets[pos].getVal(0));
            for (int i = 0; i < Bucket.COUNTER_PER_BUCKET; i++) {
                if (buckets[pos].getKey(i) == key) {
                    matched = i;
                    break;
                }
                if (buckets[pos].getKey(i) == 0 && empty == -1) {
                    empty = i;
                }
                if (minCounterVal > getCounterVal(buckets[pos].getVal(i))) {
                    minCounter = i;
                    minCounterVal = getCounterVal(buckets[pos].getVal(i));
                }
            }

            if (matched != -1) {
                buckets[pos].setVal(matched, buckets[pos].getVal(matched) + val);
                return new Pair<>(0,new Pair<>(0,0));
            }

            if (empty != -1) {
                buckets[pos].setKey(empty, key);
                buckets[pos].setVal(empty, val);
                return new Pair<>(0,new Pair<>(0,0));
            }
        } while (false);

        int guardVal = nCounters[pos];
        guardVal = updateGuardVal(guardVal);

        if (!judgeIfSwap(getCounterVal(minCounterVal), guardVal)) {
            nCounters[pos] = guardVal;
            return new Pair<>(2,new Pair<>(0,0));
        }

        int swapKey = buckets[pos].getKey(minCounter),swapVal = buckets[pos].getVal(minCounter);

        nCounters[pos] = 0;
        buckets[pos].setKey(minCounter, key);
        buckets[pos].setVal(minCounter, val);

        return new Pair<>(1,new Pair<>(swapKey,swapVal));
    }

    @Override
    public Map<Integer, Integer> queryAll() {
        Map<Integer, Integer> mp = new HashMap<>();
        for (int i = 0; i < bucketNum; i++) {
            for (int j = 0; j < Bucket.COUNTER_PER_BUCKET; j++) {
                if (buckets[i].getKey(j) != 0) {
                    mp.put(buckets[i].getKey(j), buckets[i].getVal(j));
                }
            }
        }
        return mp;
    }

    @Override
    public Integer query(Integer key) {
        int pos = (int)(hsh.run(utils.intToByteArray(key)) % bucketNum);
        for (int i = 0; i < Bucket.COUNTER_PER_BUCKET; ++i) {
            if (buckets[pos].getKey(i) == key) {
                return getCounterVal(buckets[pos].getVal(i));
            }
        }
        return 0;
    }

    @Override
    public void initial() {
        super.initial();
        java.util.Arrays.fill(nCounters, 0);
    }

    @Override
    public void initial(TopkPart refer) {
        super.initial(refer);
        java.util.Arrays.fill(nCounters, 0);
    }
}
