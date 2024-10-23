package LETFramework;


import LETFramework.Topk.TopkPart;
import LETFramework.USketch.Univmon;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class LETFramework {
    static int level_num = 14;
    protected int type, d;
    protected double heavyRatio, gama;
    protected TopkPart topk;
    protected Univmon univ;

    public abstract void clear();

    public abstract void initial();

    public abstract void insert(int key, int val);

    public abstract void batchInsert(ArrayList<Integer> keys);

    public abstract int query(int key);

    public abstract double gsum(Function<Double, Double> g);

    public abstract void getHeavyHitters(int threshold, List<Pair<String, Integer>> result);

    public static double gsumAdd(LETFrameworkImpl x, LETFrameworkImpl y, Function<Double, Double> g) {
        double heavy_sketch_gsum = 0;
        Map<Integer, Integer> res0 = x.heavySketchQueryAll(), res1 = y.heavySketchQueryAll();
        for (Map.Entry<Integer, Integer> p : res1.entrySet()) { //[key, val]
            res0.put(p.getKey(), res0.getOrDefault(p.getKey(), 0) + p.getValue());
        }
        for (Map.Entry<Integer, Integer> p : res0.entrySet()) { //[key, val]
            heavy_sketch_gsum += g.apply((double) p.getValue());
        }
        double univmon_gsum = Univmon.gsumAdd(x.univ, y.univ, g, level_num);
        return univmon_gsum + heavy_sketch_gsum;
    }

    public static double gsumMul(LETFrameworkImpl x, LETFrameworkImpl y, Function<Double, Double> g) {
        double heavy_sketch_gsum = 0;
        Map<Integer, Integer> res0 = x.heavySketchQueryAll(), res1 = y.heavySketchQueryAll(), res = new HashMap<>();
        for (Map.Entry<Integer, Integer> p : res1.entrySet()) { //[key, val]
            res.put(p.getKey(), res0.getOrDefault(p.getKey(), 0) * p.getValue());
        }
        for (Map.Entry<Integer, Integer> p : res.entrySet()) { //[key, val]
            heavy_sketch_gsum += g.apply((double) p.getValue());
        }
        double univmon_gsum = Univmon.gsumAdd(x.univ, y.univ, g, level_num);
        return univmon_gsum + heavy_sketch_gsum;
    }

    public static LETFramework create(int memory_in_byte, double heavy_ratio, int type,
                                      int d, double cu_ratio) {
        return new LETFrameworkImpl(memory_in_byte, heavy_ratio, type, d, cu_ratio);
    }

    public static enum Version {
        V1(1);

        private final int versionNumber;

        private Version(int versionNumber) {
            this.versionNumber = versionNumber;
        }

        int getVersionNumber() {
            return this.versionNumber;
        }
    }
}
