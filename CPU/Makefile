FILES = frequencyEstimation heavyhitterDetection similarity throughput estimation

all: $(FILES) 

frequencyEstimation: frequencyEstimation.cpp
	g++  frequencyEstimation.cpp common/BOBHash32.cpp -o frequencyEstimation -g -pthread -lzmq

heavyhitterDetection: heavyhitterDetection.cpp
	g++  heavyhitterDetection.cpp common/BOBHash32.cpp -o heavyhitterDetection -g -pthread -lzmq

similarity: similarity.cpp
	g++ similarity.cpp common/BOBHash32.cpp -o similarity  -g -pthread -lzmq

throughput: throughput.cpp
	g++ throughput.cpp common/BOBHash32.cpp -o throughput  -g -pthread -lzmq

estimation: estimation.cpp
	g++ estimation.cpp common/BOBHash32.cpp -o estimation  -g -pthread -lzmq


clean:
	rm $(FILES)
