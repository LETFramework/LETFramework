package LETFramework;

import LETFramework.Topk.*;
import LETFramework.USketch.Univmon;

import java.io.*;
import java.util.*;
import java.util.function.Function;

import javafx.util.Pair;

public class LETFrameworkImpl extends LETFramework implements Serializable {


    static int GetCounterVal(int val) {
        return (val) & 0x7FFFFFFF;
    }

    public LETFrameworkImpl(Integer memory_in_byte, double heavy_ratio, Integer type,
                            Integer d, double cu_ratio) {
        this.heavyRatio = heavy_ratio;
        this.type = type;
        this.d = d;
        this.gama = 1 - cu_ratio;
        topk = TopkPart.create(type, (int) (heavy_ratio * memory_in_byte));
        univ = new Univmon((int) (memory_in_byte * (1 - heavy_ratio)), (heavy_ratio > 1e-5) ? d : 5, gama, 14);
    }

    public void clear() {
        univ.clear();
    }

    public void initial() {
        topk.initial();
        univ.initial();
    }

    public void initial(LETFrameworkImpl refer) {
        topk.initial(refer.topk);
        univ.initial(refer.univ);
    }

    public void insert(int key, int val) {
        Pair<Integer, Integer> swap_pair = heavySketchInsert(key, val);
        if (heavyRatio <= 1 - 1e-5 && swap_pair.getValue() != 0) {
            univ.insert(swap_pair.getKey(), swap_pair.getValue());
        }
    }

    public void batchInsert(ArrayList<Integer> keys) {

        for (Integer key : keys) {
            insert(key, 1);
        }
    }

    public int query(int key) {
        int heavy_result = (heavyRatio > 1e-5) ? topk.query(key) : 0;
        if (heavyRatio<1-1e-5 && (GetCounterVal(heavy_result) == 0 || ((heavy_result&0x80000000)!=0))) {
            int light_result = univ.query(key);
            return (int) GetCounterVal(heavy_result) + light_result;
        }
        return (int)GetCounterVal(heavy_result);
    }

    public double gsum(Function<Double, Double> g) {
        double heavy_sketch_gsum = 0;
        Map<Integer, Integer> res = heavySketchQueryAll();
        for (Map.Entry<Integer, Integer> p : res.entrySet()) {
            heavy_sketch_gsum += g.apply(Double.valueOf(p.getValue()));
        }

        double univmon_gsum = heavyRatio > 1 - 1e-5 ? 0 : univ.gsum(g);
        return (double) (heavy_sketch_gsum + univmon_gsum);
    }
    public void getHeavyHitters(int threshold, List<Pair<String, Integer>> result) {
        List<Pair<Integer, Integer>> univmon_result = new ArrayList<>();
        Map<Integer, Integer> mp = heavySketchQueryAll();
        univ.getHeavyHitters(threshold, univmon_result);
        for (Pair<Integer, Integer> p : univmon_result) {
            mp.put(p.getKey(), mp.getOrDefault(p.getKey(), 0) + p.getValue());
        }
        for (Map.Entry<Integer, Integer> p : mp.entrySet()) {
            if (p.getValue() >= threshold) {
                result.add(new Pair<>(p.getKey().toString(), p.getValue()));
            }
        }
    }
    protected Map<Integer, Integer> heavySketchQueryAll() {
        Map<Integer, Integer> res0 = topk.queryAll();
        Map<Integer, Integer> res = new HashMap<>();
        for (Map.Entry<Integer, Integer> p : res0.entrySet()) {
            int heavyResult = p.getValue();
            res.put(p.getKey(), (int)GetCounterVal(heavyResult));
        }
        return res;
    }
    private Pair<Integer, Integer> heavySketchInsert(int key, int f) {
        if (heavyRatio <1e-5)
            return new Pair<>(key, f);

        Pair<Integer,Pair<Integer,Integer>> result = topk.insert(key,  f);
        Integer swap_key=result.getValue().getKey(),swap_val = result.getValue().getValue();
        switch (result.getKey())
        {
            case 1: return new Pair<>(swap_key, (int)GetCounterVal(swap_val));
            case 2: return new Pair<>(key, f);
            case 0:default: return new Pair<>(0, 0);
        }
    }
}
