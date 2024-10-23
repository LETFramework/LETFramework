package LETFramework.USketch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import LETFramework.USketch.CountHeap;
import common.BOBHash32;
import common.utils;
import javafx.util.Pair;


public class Univmon {
    private CountHeap[] sketches;
    private BOBHash32[] hash;
    private int elementNum = 0;
    private int level, d;
    private double gama;
    private int memInBytes;

    public Univmon(int memInBytes, int d, double gama, int level) {
        this.level = level;
        this.memInBytes = memInBytes;
        this.d = d;
        this.gama = gama;
        sketches = new CountHeap[level];
        hash = new BOBHash32[level];
        initial();
    }

    public void initial(Univmon refer) {
        if (memInBytes == 0)
            return;
        int memForSk = (int) memInBytes;
        int mem = memForSk / level;
        for (int i = 0; i < level; i++) {
            sketches[i] = new CountHeap(refer.sketches[i]);
            hash[i] = new BOBHash32(refer.hash[i]);
        }
    }

    public void initial() {
        if (memInBytes == 0)
            return;
        int memForSk = (int) memInBytes;
        int mem = memForSk / level;
        List<Integer> rdList = BOBHash32.getRandomPrimeIndexList(level);
        for (int i = 0; i < level; i++) {
            sketches[i] = new CountHeap((int) ((1 - gama) * mem), (int) (gama * mem), d);
            hash[i] = new BOBHash32(rdList.get(i));
        }
    }

    public void clear() {
        if (memInBytes == 0)
            return;
        for (int i = 0; i < level; i++) {
            sketches[i] = null;
            hash[i] = null;
        }
    }

    public void insert(int key, int f) {
        elementNum += f;
        int hashVal;
        sketches[0].insert(intToByteArray(key), f);
        for (int i = 1; i < level; i++) {
            hashVal = (int)(hash[i].run(Integer.valueOf(key).toString().getBytes()) & 2);
            if (hashVal != 0) {
                sketches[i].insert(intToByteArray(key), f);
            } else {
                break;
            }
        }
    }

    public double gsum(Function<Double, Double> g) {
        int  coe;
        int hashVal;
        List<KV> result = new ArrayList<>();
        double[] Y = new double[level];
        for (int i = level - 1; i >= 0; i--) {
            sketches[i].getTopKWithFrequency(result);
            Y[i] = (i == level - 1) ? 0 : 2 * Y[i + 1];
            for (KV kv : result) {
                if (kv.value == 0) {
                    continue;
                }
                hashVal = (i == level - 1) ? 1 : (int)(hash[i + 1].run(utils.intToByteArray(kv.key)) % 2);
                coe = (i == level - 1) ? 1 : 1 - 2 * hashVal;
                Y[i] += coe * g.apply((double) kv.value);
            }
        }
        return Y[0];
    }

    public double getCardinality() {
        return gsum(x -> 1.0);
    }

    public double getEntropy() {
        double sum = gsum(x -> x == 0 ? 0 : x * Math.log(x) / Math.log(2));
        return Math.log(elementNum) / Math.log(2) - sum / elementNum;
    }

    public void getHeavyHitters(int threshold, List<Pair<Integer, Integer>> ret) {
        Map<Integer, Integer> results = new HashMap<>();
        List<KV> vecTopK = new ArrayList<>();
        for (int i = level - 1; i >= 0; --i) {
            sketches[i].getTopKWithFrequency(vecTopK);
            for (KV kv : vecTopK) {
                results.put(kv.key, Math.max(results.getOrDefault(kv.key, 0), kv.value));
            }
        }

        ret.clear();
        for (Map.Entry<Integer, Integer> kv : results.entrySet()) {
            if (kv.getValue() >= threshold) {
                ret.add(new Pair<>(kv.getKey(), kv.getValue()));
            }
        }
    }

    public static double gsumAdd(Univmon x, Univmon y, Function<Double, Double> g, int level) {
        int coe;
        int hashVal;
        List<KV> result0 = new ArrayList<>();
        List<KV> result1 = new ArrayList<>();
        double[] Y = new double[level];
        for (int i = level - 1; i >= 0; i--) {
            Map<Integer, Integer> mp = new HashMap<>();
            x.sketches[i].getTopKWithFrequency(result0);
            y.sketches[i].getTopKWithFrequency(result1);
            for (KV p : result0) {
                mp.put(p.key, p.value);
            }
            for (KV p : result1) {
                mp.put(p.key, mp.getOrDefault(p.key, 0) + p.value);
            }
            Y[i] = (i == level - 1) ? 0 : 2 * Y[i + 1];
            CountHeap combineSketch = new CountHeap(x.sketches[i]);
            CountHeap.join(x.sketches[i], y.sketches[i], combineSketch);
            for (Map.Entry<Integer, Integer> p : mp.entrySet()) {
                double estimate = combineSketch.query(utils.intToByteArray(p.getKey()));
                hashVal = (i == level - 1) ? 1 : (int)(x.hash[i + 1].run(utils.intToByteArray(p.getKey())) % 2);
                coe = (i == level - 1) ? 1 : 1 - 2 * hashVal;
                Y[i] += coe * g.apply(estimate);
            }
        }
        return Y[0];
    }

    public static double gsumMul(Univmon x, Univmon y, Function<Double, Double> g, int level) {
        int coe;
        int hashVal;
        List<KV> result0 = new ArrayList<>();
        List<KV> result1 = new ArrayList<>();
        double[] Y = new double[level];
        for (int i = level - 1; i >= 0; i--) {
            Map<Integer, Integer> mp0 = new HashMap<>();
            Map<Integer, Integer> mp = new HashMap<>();
            x.sketches[i].getTopKWithFrequency(result0);
            y.sketches[i].getTopKWithFrequency(result1);
            for (KV p : result0) {
                mp0.put(p.key, p.value);
            }
            for (KV p : result1) {
                if (mp0.containsKey(p.key))
                    mp.put(p.key, mp0.get(p.key) * p.value);
            }
            Y[i] = (i == level - 1) ? 0 : 2 * Y[i + 1];
            for (Map.Entry<Integer, Integer> p : mp.entrySet()) {
                if (p.getValue() <= 0) {
                    continue;
                }
                hashVal = (i == level - 1) ? 1 : (int)(x.hash[i + 1].run(utils.intToByteArray(p.getKey())) % 2);
                coe = (i == level - 1) ? 1 : 1 - 2 * hashVal;
                Y[i] += coe * g.apply((double) p.getValue());
            }
        }
        return Y[0];
    }

    public int query(int key) {
        return Math.max(0, sketches[0].query(intToByteArray(key)));
    }

    private byte[] intToByteArray(int value) {
        return new byte[] {
                (byte)(value >>> 24),
                (byte)(value >>> 16),
                (byte)(value >>> 8),
                (byte)value
        };
    }
}