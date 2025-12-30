#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <unistd.h>

#define READY_TAG 2
#define WORK_TAG 1
#define STOP_TAG 0

#define CMD_SIZE 100
#define MAX_RESULT_SIZE 500000

int is_prime(int n) {
    if (n<2) return 0;
    if (n==2) return 1;
    if (n%2==0) return 0;
    for (int i=3;i*i<=n;i+=2) {
        if (n%i==0) return 0;
    }
    return 1;
}

int count_primes(int n) {
    int count=0;
    if (n>=2) count++;
    for (int i=3;i<=n;i+=2) {
        if (is_prime(i))
            count++;
    }
    return count;
}

int count_prime_divisors(int n) {
    int count = 0;
    int temp=n;

    if (temp%2==0) {
        count++;
        while (temp%2==0) {
            temp/=2;
        }
    }
    for (int i=3;i*i<=temp;i+=2) {
        if (temp%i==0) {
            count++;
            while (temp%i==0) temp/=i;
        }
    }

    if (temp>2) {
        count++;
    }
    return count;
}

void swap(char *x, char *y) {
    char temp;
    temp = *x;
    *x = *y;
    *y = temp;
}

void permute(char *a, int left,int right, char *buffer, int *offset) {
    int i;
    if (left==right) {
        *offset+=sprintf(buffer+*offset,"%s\n",a);
    }
    else {
        for (i = left; i<=right; i++) {
            swap((a + left), (a + i));
            permute(a,left+1,right,buffer,offset);
            swap((a+left), (a+i));
        }
    }
}

void solve_anagrams(char *str,char *output_buffer, int*current_offset) {
    int n=strlen(str);
    permute(str,0,n-1,output_buffer,current_offset);
}

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

void write_result(char *response_buffer) {
    char client_id[20];

    sscanf(response_buffer,"%s",client_id);

    char *actual_result=response_buffer + strlen(client_id)+1;

    char filename[50];
    sprintf(filename,"%s.out",client_id);

    FILE *file = fopen(filename,"w");

    if (file==NULL) {
        printf("Error opening output file %s\n",filename);
        return;
    }

    fprintf(file,"%s\n",actual_result);
    if (fclose(file)!=0) {
        printf("Error closing file %s\n",filename);
    }

    printf("[Master] Wrote result to %s\n",filename);
    fflush(stdout);

}

void master_process(int nProcesses, char *inputFileName) {

    printf("[Master] has started. Workers: %d.\n", nProcesses-1);
    fflush(stdout);

    char *response=(char*)malloc(MAX_RESULT_SIZE*sizeof(char));

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
        MPI_Recv(response,MAX_RESULT_SIZE,MPI_CHAR,MPI_ANY_SOURCE,READY_TAG,MPI_COMM_WORLD,&status);
        write_result(response);

        int worker_id = status.MPI_SOURCE;

        MPI_Send(line,CMD_SIZE,MPI_CHAR,worker_id,WORK_TAG,MPI_COMM_WORLD);
    }

    if (fclose(file)!=0) {
        printf("Failed to close file %s\n",inputFileName);
        return;
    }

    while (active_workers>0) {
        MPI_Recv(response,MAX_RESULT_SIZE,MPI_CHAR,MPI_ANY_SOURCE,READY_TAG,MPI_COMM_WORLD,&status);
        write_result(response);

        int worker_id = status.MPI_SOURCE;

        MPI_Send(NULL,0,MPI_CHAR,worker_id,STOP_TAG,MPI_COMM_WORLD);
        active_workers--;
    }

    free(response);
    printf("[Master] All commands executed. Shutting down.\n");
    fflush(stdout);
}

void worker_process(int rank) {

    char input[CMD_SIZE];
    MPI_Status status;
    char *large_buffer=(char*)malloc(MAX_RESULT_SIZE * sizeof(char));
    if (large_buffer==NULL) {
        printf("[Worker %d] Error allocating memory.\n",rank);
        MPI_Abort(MPI_COMM_WORLD,1);
    }

    while (1) {
        MPI_Recv(input,CMD_SIZE,MPI_CHAR,0,MPI_ANY_TAG,MPI_COMM_WORLD,&status);
        if (status.MPI_TAG == STOP_TAG) {

            printf("[Worker %d] I received the stopping signal. Exiting!\n",rank);
            fflush(stdout);
            break;
        }
        else if (status.MPI_TAG == WORK_TAG) {

            char client_id[20];
            char command[20];
            char argument[50];

            sscanf(input,"%s %s %s",client_id,command,argument);

            int number=0;
            int result=0;
            int offset=sprintf(large_buffer,"%s\n",client_id);

            if (strcmp(command,"PRIMES")==0) {
                number=atoi(argument);
                result=count_primes(number);
                offset+=sprintf(large_buffer+offset,"%d",result);
            }
            else if (strcmp(command,"PRIMEDIVISORS")==0) {
                number=atoi(argument);
                result=count_prime_divisors(number);
                offset+=sprintf(large_buffer+offset,"%d",result);
            }
            else if (strcmp(command,"ANAGRAMS")==0) {

                solve_anagrams(argument,large_buffer,&offset);
            }

            MPI_Send(large_buffer,offset+1,MPI_CHAR,0,READY_TAG,MPI_COMM_WORLD);

            printf("[Worker %d] Finished %s. Sent %d bytes.\n",rank,command,offset);
            fflush(stdout);

        }
    }
    free(large_buffer);
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