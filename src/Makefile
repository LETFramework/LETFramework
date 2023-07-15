FILES = frequencyEstimation heavyhitterDetection similarity throughput estimation

all: $(FILES) 

frequencyEstimation: frequencyEstimation.cpp
	g++  frequencyEstimation.cpp common/BOBHash32.cpp -o frequencyEstimation -g

heavyhitterDetection: heavyhitterDetection.cpp
	g++  heavyhitterDetection.cpp common/BOBHash32.cpp -o heavyhitterDetection -g

similarity: similarity.cpp
	g++ similarity.cpp common/BOBHash32.cpp -o similarity  -g

throughput: throughput.cpp
	g++ throughput.cpp common/BOBHash32.cpp -o throughput  -g

estimation: estimation.cpp
	g++ estimation.cpp common/BOBHash32.cpp -o estimation  -g


clean:
	rm $(FILES)
