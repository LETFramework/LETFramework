# CAIDA Dataset

We provide a fragment of the CAIDA dataset with 8 bit timestamp.Each 21-byte string is a 6-tuple in the format of (srcIP, srcPort, dstIP, dstPort, protocol,timestamp). When reading a single CAIDA dataset file we will treat the first and second halves as two data streams.