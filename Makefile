all:
	g++ -std=c++17 -g -lz decompressor.cc main.cc reader.cc tile.cc -o fmd_dissector
