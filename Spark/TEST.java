import common.utils;
import javafx.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.util.sketch.CountMinSketch;
import LETFramework.LETFramework;
import scala.Tuple2;

import java.io.*;
import java.util.*;
/**
 * Created by hadoop on 17-4-4.
 */
public class TEST {
    static String folder ="../../../../datasets/caida/" /*"../datasets/caida/"*/;
    static String folder1 = "hdfs://master:9000/sketch/";
    static String[] filenames = {"trace3.dat", "130000.dat"};

    static Double gsum(Double x)
    {
        return x >= 0 ? x : 0;
    }
    public static String test_TH(ArrayList<Integer> items) throws IOException {
        Map<Integer, Integer> mp = new HashMap<Integer, Integer>();
        for (Integer item : items) {
            if (mp.containsKey(item)) {
                mp.put(item, mp.get(item) + 1);
            } else {
                mp.put(item, 1);
            }
        }
        System.out.println("The number of packet:" + items.size());
        System.out.println("The number of flow:" + mp.size());
        String ret=new String();
        String[] sketches=new String[]{"E-LETFramework","H-LETFramework","S-LETFramework","L-LETFramework"};
        ret+="MEM(KB),E-LETFramework,H-LETFramework,S-LETFramework,L-LETFramework\n";
        int T=5;
        for (int j = 100; j < 1000; j+=200) {
            int memory=j*1000;
            ret+=Integer.valueOf(j).toString()+",";
            System.out.println(j+"KB");
            for (int i = 0; i < 4; i++) {
                long elapse_time=0;
                double aae=0;
                for (int loop = 0; loop < T; loop++) {
                    LETFramework sketch = LETFramework.create(memory,0.6,i,1,0.9);
                    sketch.initial();
                    long stime = System.nanoTime();
                    sketch.batchInsert(items);
                    long etime = System.nanoTime();
                    elapse_time+=etime-stime;
                    if (loop==0) {
                        for (Map.Entry<Integer,Integer> p:mp.entrySet()) {
                            Integer predict=sketch.query(p.getKey()),truth=p.getValue();
                            aae+=Math.abs(predict-truth);
                        }
                    }
                }
                elapse_time/=T;
                ret+=Double.valueOf(1.0*items.size()/elapse_time*1000).toString()+",";
                System.out.println(String.format("%s %f MIPS AAE=%f",sketches[i],1.0*items.size()/elapse_time*1000,aae/mp.size()));
            }
            ret+="\n";
        }
        return ret;
    }

    public static void main(String[] args) throws IOException {
        /*String FILE =folder + filenames[0];
        Pair<ArrayList<Integer>, ArrayList<utils.TIMESTAMP>> ret=utils.read_data(FILE,1000000000);
        test_TH(ret.getKey());*/

        SparkConf conf = new SparkConf();
        conf.setAppName("ES").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, PortableDataStream> linesRDD = sc.binaryFiles(folder1 + filenames[1], 1);
        linesRDD.map(new Function<Tuple2<String, PortableDataStream>, ArrayList<Integer>>() {
            @Override
            public ArrayList<Integer> call(Tuple2<String, PortableDataStream> stringPortableDataStreamTuple2) throws Exception {
                PortableDataStream a = stringPortableDataStreamTuple2._2;
                DataInputStream fis = a.open();
                ArrayList<Integer> items = new ArrayList<Integer>();
                utils.DATA_TYPE it = new utils.DATA_TYPE();
                utils.TIMESTAMP timestamp = new utils.TIMESTAMP();
                while (true) {
                    try {
                        fis.readFully(it.array, 0, it.array.length);
                        fis.readFully(timestamp.array, 0, timestamp.array.length);
                    } catch (EOFException e) {
                        break;
                    }
                    items.add(utils.ByteToInt(it.array));
                }
                System.out.println("\n" + items.size() + "\n");
                return items;
            }
        }).map(new Function<ArrayList<Integer>, String>() {
            @Override
            public String call(ArrayList<Integer> arrayList) throws Exception {
                return test_TH(arrayList);
            }
        }).saveAsTextFile(folder1 + "output");
    }
}