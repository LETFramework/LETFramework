package LETFramework.Topk;

import common.BOBHash32;
import common.utils;
import javafx.util.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class HeavyGuardianBased extends TopkPart implements Serializable {
    private int bucketNum;
    private int[] nCounters;
    private BOBHash32 hsh;
    private Random rnd;
    private Bucket[] buckets;
    private static final double HK_b = 1.08;
    private static final boolean JUDGE_IF_SWAP(int min_val, int guard_val) {
        return guard_val > min_val;
    }
    public HeavyGuardianBased(int memInBytes) {
        bucketNum = memInBytes/Bucket.SIZE;
        buckets = new Bucket[bucketNum];
        nCounters = new int[bucketNum];
        hsh = new BOBHash32();
        rnd=new Random();
        for (int i = 0; i < bucketNum; i++) {
            buckets[i]=new Bucket();
        }
    }
    @Override
    public Pair<Integer, Pair<Integer, Integer>> insert(Integer key, Integer val) {
        int pos = (int)(hsh.run(utils.intToByteArray(key)) % bucketNum);
        int min_counter_val;
        int min_counter;

        do {
            int matched = -1, empty = -1;
            min_counter = 0;
            min_counter_val = getCounterVal(buckets[pos].getVal(0));
            for (int i = 0; i < Bucket.COUNTER_PER_BUCKET; i++) {
                if (buckets[pos].getKey(i) == key) {
                    matched = i;
                    break;
                }
                if (buckets[pos].getKey(i) == 0 && empty == -1) {
                    empty = i;
                }
                if (min_counter_val > getCounterVal(buckets[pos].getVal(i))) {
                    min_counter = i;
                    min_counter_val = getCounterVal(buckets[pos].getVal(i));
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
            int t=1;
        } while (false);

        int guard_val = nCounters[pos];
        if (rnd.nextInt((int) Math.pow(HK_b, min_counter_val - nCounters[pos])) == 0) {
            guard_val = updateGuardVal(guard_val);
        }

        if (!JUDGE_IF_SWAP(getCounterVal(min_counter_val), guard_val)) {
            nCounters[pos] = guard_val;
            return new Pair<>(2,new Pair<>(0,0));
        }

        int swap_key = buckets[pos].getKey(min_counter),swap_val = buckets[pos].getVal(min_counter);

        nCounters[pos] = 0;
        buckets[pos].setKey(min_counter, key);
        buckets[pos].setVal(min_counter, val);

        return new Pair<>(1,new Pair<>(swap_key,swap_val));
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
                return buckets[pos].getVal(i);
            }
        }
        return 0;
    }




    @Override
    public void initial() {
        super.initial();
        for (int i = 0; i < nCounters.length; i++) {
            nCounters[i] = 0;
        }
    }

    @Override
    public void initial(TopkPart _refer) {
        super.initial(_refer);
        for (int i = 0; i < nCounters.length; i++) {
            nCounters[i] = 0;
        }
    }
}
