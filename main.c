#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

#define WORK_TAG 1
#define STOP_TAG 0

#define CMD_SIZE 100

void master_process(int nProcesses, char *inputFileName) {

    printf("[Master] has started. Workers: %d.\n", nProcesses-1);

    for (int i=1;i<nProcesses;i++) {
        MPI_Send(NULL,0,MPI_CHAR,i,STOP_TAG,MPI_COMM_WORLD);
    }

    printf("[Master] has sent the stopping signal to all workers.\n");
}

void worker_process(int rank) {

    char input[CMD_SIZE];
    MPI_Status status;

    while (1) {
        MPI_Recv(input,CMD_SIZE,MPI_CHAR,0,MPI_ANY_TAG,MPI_COMM_WORLD,&status);
        if (status.MPI_TAG == STOP_TAG) {

            printf("[Worker %d] I received the stopping signal. Good bye!\n",rank);
            break;
        }
        else if (status.MPI_TAG == WORK_TAG) {

            printf("[Worker %d] I received the command: %s\n",rank,input);
        }
    }
}

int main(int argc, char** argv) {

    int rank,nProcesses;
    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nProcesses);

    if (nProcesses<2) {
        if (rank==0) {
            fprintf(stderr,"Error: The program needs at least 2 MPI processes!\n");
        }
        MPI_Finalize();
        return 1;
    }

    if (rank==0) {
        if (argc<2) {
            printf("Execute as: mpiexec -n N %s <input_file>\n", argv[0]);
            for (int i=1;i<nProcesses;i++) {
                MPI_Send(NULL,0,MPI_CHAR,i,STOP_TAG,MPI_COMM_WORLD);
            }
        }
        else {
            master_process(nProcesses,argv[1]);
        }
    }
    else {
        worker_process(rank);
    }

    MPI_Finalize();
    return 0;
}