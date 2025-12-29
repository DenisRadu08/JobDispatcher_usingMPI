#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <unistd.h>

#define READY_TAG 2
#define WORK_TAG 1
#define STOP_TAG 0

#define CMD_SIZE 100

int get_next_command(FILE *file, char *line) {
    while (fgets(line,CMD_SIZE,file)!=NULL) {
        line[strcspn(line,"\n")] = '\0';

        if (strlen(line)==0) continue;

        if (strncmp(line,"WAIT",4)==0) {
            int seconds=atoi(line+5);
            printf("[Master] Waiting for %d seconds...\n",seconds);
            fflush(stdout);
            sleep(seconds);
        }
        else {
            return 1;
        }
    }
    return 0;
}

void master_process(int nProcesses, char *inputFileName) {

    printf("[Master] has started. Workers: %d.\n", nProcesses-1);
    fflush(stdout);

    FILE *file= fopen(inputFileName, "r");
    if (file==NULL) {
        printf("[Master] Failed to open file %s. Aborting.\n", inputFileName);
        for (int i=1;i<nProcesses;i++) {
            MPI_Send(NULL,0,MPI_CHAR,i,STOP_TAG,MPI_COMM_WORLD);
        }
        return;
    }

    char line[CMD_SIZE];
    MPI_Status status;
    int active_workers=0;
    for (int i=1;i<nProcesses;i++) {
        if (get_next_command(file,line)) {
            MPI_Send(line,CMD_SIZE,MPI_CHAR,i,WORK_TAG,MPI_COMM_WORLD);
            printf("[Master] Sending command: %s to Worker %d\n",line,i);
            fflush(stdout);
            active_workers++;
        }
        else {
            MPI_Send(NULL,0,MPI_CHAR,i,STOP_TAG,MPI_COMM_WORLD);
        }
    }

    while (get_next_command(file,line)) {
        MPI_Recv(NULL,0,MPI_CHAR,MPI_ANY_SOURCE,READY_TAG,MPI_COMM_WORLD,&status);

        int worker_id = status.MPI_SOURCE;
        printf("[Master] Worker %d is ready. Sending %s command.\n",worker_id,line);
        fflush(stdout);

        MPI_Send(line,CMD_SIZE,MPI_CHAR,worker_id,WORK_TAG,MPI_COMM_WORLD);
    }

    if (fclose(file)!=0) {
        printf("Failed to close file %s\n",inputFileName);
        return;
    }

    while (active_workers>0) {
        MPI_Recv(NULL,0,MPI_CHAR,MPI_ANY_SOURCE,READY_TAG,MPI_COMM_WORLD,&status);
        int worker_id = status.MPI_SOURCE;

        MPI_Send(NULL,0,MPI_CHAR,worker_id,STOP_TAG,MPI_COMM_WORLD);
        printf("[Master] Worker %d finished his work. Sending STOP.\n",worker_id);
        fflush(stdout);
        active_workers--;
    }

    printf("[Master] All commands executed. Shutting down.\n");
    fflush(stdout);
}

void worker_process(int rank) {

    char input[CMD_SIZE];
    MPI_Status status;

    while (1) {
        MPI_Recv(input,CMD_SIZE,MPI_CHAR,0,MPI_ANY_TAG,MPI_COMM_WORLD,&status);
        if (status.MPI_TAG == STOP_TAG) {

            printf("[Worker %d] I received the stopping signal. Exiting!\n",rank);
            fflush(stdout);
            break;
        }
        else if (status.MPI_TAG == WORK_TAG) {

            printf("[Worker %d] I received the command: %s\n",rank,input);
            fflush(stdout);

            MPI_Send(NULL,0,MPI_CHAR,0,READY_TAG,MPI_COMM_WORLD);
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