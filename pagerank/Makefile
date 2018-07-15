all:
	mpic++ -std=c++11 -O3 new.cpp -o test
	time mpiexec -np 2 ./test fl_compact.tab fl_compact_part.2 5
