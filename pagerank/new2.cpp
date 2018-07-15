#include <vector>
#include <fstream>
#include <map>
#include <chrono>
#include <typeinfo>
#include <mpi.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <cstdlib>
#include <new>
#include <cstring>

using namespace std::chrono;
using namespace std;  

void dompi(int r,vector<pair<int,int>> adjacencylist,vector<int> rank,vector<int> degree,double** creditlist,int prank)
{
	      
	for(int j=0;j<adjacencylist.size();j++)
		{
			if(prank==rank[adjacencylist[j].first])
			{
				creditlist[r][adjacencylist[j].first]+=(creditlist[r-1][adjacencylist[j].second]/degree[adjacencylist[j].second]);
			}


		}

}



int main(int argc,char* argv[])

{
	  try{
		int proc_rank,num_procs;

		MPI_Init(&argc, &argv);
		MPI_Comm_rank(MPI_COMM_WORLD, &proc_rank);
		MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
		int val1,val2,val3,val4,val5,val6,val7,val8;
		int maxnodeid;
		const int r=atoi(argv[3]);
		double process_start=MPI_Wtime();
		double start=MPI_Wtime();
		ifstream ifs2(argv[1]);
		ifstream ifs(argv[2]);
		while(ifs>>val1>>val2>>val3)
		{
			if(maxnodeid<val1)
				maxnodeid=val1;
		}

		ifs.clear();
		ifs.seekg(0,ios::beg);
		vector<int> rank(maxnodeid+1);
		vector<int> degree(maxnodeid+1);
		while(ifs>>val4>>val5>>val6)
		{
			rank[val4]=val6;
			degree[val4]=val5;
		}


		vector<pair<int,int>> adjacencylist;
		while(ifs2>>val7>>val8)
		{
			adjacencylist.push_back(pair<int,int>(val7,val8));
			adjacencylist.push_back(pair<int,int>(val8,val7));

		}
		double end=MPI_Wtime();
		cout << "Time to read input files, partition " << proc_rank << " = " << end - start <<endl;
			double ** creditlist = new double*[r+1];
 			for(int i = 0; i <=r; i++)
    		{
        	creditlist[i] = new double[maxnodeid+1];
    		}
 						   
		for(int j=0;j<=r;j++){
			for (int k=0;k<=maxnodeid;k++)
			{
				if(j==0){
				creditlist[0][k]=1.0f;
			}
			else{
				creditlist[j][k]=0.0f;
				}
													             
				}
			}
								         

			for(int i=1;i<=r;i++)
				{
					double start=MPI_Wtime();
					dompi(i,adjacencylist, rank, degree, creditlist,proc_rank);
					double* output=new double[maxnodeid+1];
					
					
					MPI_Allreduce(&creditlist[i][0],&output[0] , maxnodeid+1, MPI_DOUBLE, MPI_SUM,MPI_COMM_WORLD);
					for(int k=0;k<=maxnodeid;k++){
						creditlist[i][k]=output[k];
					}
					double end=MPI_Wtime();
					for (int j= 0; j < num_procs; j++) {
					cout << "Time for round " << i<< " partition " << proc_rank<< "=" << end - start << endl;
					}

					delete[] output;
														          
					}

					string name=to_string(proc_rank)+".out";
					FILE* f =fopen(name.c_str(),"w");

					for(int d=0;d<degree.size();d++)
					{

					if(rank[d]==proc_rank&& degree[d]!=0)
					{
						fprintf(f,"%d\t%d\t",d,degree[d]);

						for(int i=1;i<=r;i++)
							{
								fprintf(f,"\t%lf\t",creditlist[i][d]);
																										}
								fprintf(f,"\n");


							}

					}
					fclose(f);
					for(int i = 0; i <= r; i++)
    				{
        			delete[] creditlist[i];
    				}
    				delete[] creditlist;
    				double process_end = MPI_Wtime();
					cout << "Total time for round" << proc_rank << "= " << process_end - process_start << endl;
 
					MPI_Finalize();
	  }
	  catch (std::bad_alloc& ba)
		    {
			        std::cerr << "bad_alloc caught: " << ba.what() << '\n';
				  }
	   return 0;

}

