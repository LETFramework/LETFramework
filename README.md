# LETFramework: Let the Universal Sketch be Accurate

## Introduction

Sketching algorithms are considered as promising solutions for approximate query tasks on large volumes of data streams. An ideal general-purpose data processing engine requires a sketch to achieve (1) high genericness in supporting a broad range of query tasks; (2) high fidelity in providing accuracy guarantee; and (3) high performance in practice. Although the universal sketch achieves high genericness and fidelity, its accuracy falls short of expectations. In this paper, we propose LETFramework (short for Lossless ExTraction Framework) to optimize the performance of the universal sketch. With the key technique of lossless extraction, LETFramework losslessly extracts the main body of the frequent items while stores the remaining information in the universal sketch, thereby achieving higher accuracy while maintaining high fidelity.We further introduce a unified methodology to incorporate the substitution strategies from top-k algorithms into LETFramework. Experiment results show that, LETFramework outperforms the universal sketch, achieving accuracy improvements ranging from 1 to 3 orders of magnitude on most query tasks and up to 15.73 times higher throughput.

## About this repo

- `dataset/`:The sample datasets extracted from the real-world datasets used in our experiments,including IP Trace Dataset(CAIDA)、Webdocs Dataset and Synthetic Dataset.
- `CPU/`:LETFramework and other algorithms implemented in C++ on CPU
- `Spark/`:LETFramework and other algorithms implemented in JAVA on Spark
## Requirements

- cmake
- g++
- C++11

## Datasets

- `/CAIDA` We provide a fragment of the CAIDA dataset with 8 bit timestamp.Each 21-byte string is a 6-tuple in the format of (srcIP, srcPort, dstIP, dstPort, protocol,timestamp).
- `/zipf` We generate a series of synthetic datasets that follow the Zipf distribution. The skewness of the datasets range from 0.0 to 1.0. Each dataset contains approximately 1.0M flows, 32.0M items. The length of each item ID is 4 bytes.
- `/webdocs` We provide a fragment of the webdocs dataset with 8 bit timestamp. Each 16-byte string is a 2-tuple in the format of (ID, timestamp).

In query on multiple data streams, which is implemented in `src/similarity.cpp`, we will treat the first and second halves of dataset file as two data streams.

For more details, please refer to our paper.

## How to make and test

You can use the following commands to build and then you can test the algorithms in src.

```shell
$ cd src
$ make
```

To accommodate a wider range of testing, you should add the following parameters in sequence after the executable file:

1. `configFilePath`: configFile records the parameters of the LETFramework,including:

   - `lambda`: A float,the items whose frequencies are larger than a predefined threshold $\Lambda$ are defined as heavy hitter
   - `total_mem`: An integer, representing the memory size (in bytes) used by the LETFramework.
   - `topk`: A float , representing the memory ratio of the Top-k part
   - `countSketch`: A float, representing the memory ratio of the CountSketech **in USketch part**
   - `d`: An integer ranging from 1 to 10, representing the number of arrays of the Count sketches in the USketch part

   Please refer to the "config.txt" file in the "/src" directory to write your configuration file that meets the requirements.
2. `datasetPath`: The path of the dataset you want to run.
3. `datasetType`: An integer ranging from 0 to 2 represents the type of dataset you want to run, where 0 represents the CAIDA dataset, 1 represents the zipf dataset, and 2 represents the webdocs dataset.
4. `g`: The G-sum function you can run includes "cardinality","sum","entropy" and "sqr" representing $g(x) = x^0$, $g(x) = x$, $g(x) = x log x$, and $g(x) = x^2$, respectively.It should be noted that, only when testing the similarity file (Query on Multiple Data Streams), the G-sum function changes to "cosine," "jaccard," and "innerProduction."

   For example, the following commands are all valid commands:

   ```shell
   $ ./estimation config.txt ../dataset/CAIDA.dat 0 entropy
   $ ./throughput config.txt ../dataset/CAIDA.dat 0 sum
   $ ./heavyhitterDetection ../a.txt ../webdocs.dat 2 sum
   $ ./similarity config.txt ../dataset/zipf.dat 1 jaccard
   $ ./frequencyEstimation ./b.txt ../dataset/zipf.dat 1 sum
   ```
   ### Output format

   Our program will print the statistics about the input dataset and the parameters of the candidate algorithms at the command-line interface.

## What APIs does LETFramework supply
The LETFramework offers the following APIs:
1. `insert`:Inserting a user-specified number of keys into the LETFramework.
2. `query`: Query the number of frequency of a specific key in the LETFramework.
3. `getHeavyHitter`: reporting flows whose sizes are larger than a predefined threshold.
4. `gsum`: Estimating G-sum $\sum_x g\left(f_x\right)$, where the function g is provided by the user.
5. `gsumAdd`: Estimating G-sum $\sum_x g\left(f_A(x)+f_B(x)\right)$, where the function g is provided by the user and 
$f_A(x)$ is the frequency of item x in the data stream A, and $f_B(x)$ is the frequency of item x in the data stream B.
6. `gsumMul`: Estimating G-sum $\sum_x g\left(f_A(x)*f_B(x)\right)$, where the function g is provided by the user and 
$f_A(x)$ is the frequency of item x in the data stream A, and $f_B(x)$ is the frequency of item x in the data stream B.
