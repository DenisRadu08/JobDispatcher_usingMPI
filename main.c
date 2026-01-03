#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <unistd.h>

#define READY_TAG 2
#define WORK_TAG 1
#define STOP_TAG 0

#define CMD_SIZE 100
#define MAX_RESULT_SIZE 5000000
#define DEBUG

double START_TIME=0.0;
#define TIME (MPI_Wtime() - START_TIME)

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

void permute_for_serial(char *a,int left, int right, FILE *f) {
    if (left==right) {
        fprintf(f,"%s\n",a);
    }
    else {
        for (int i=left; i<=right; i++) {
            swap((a+left), (a+i));
            permute_for_serial(a,left+1,right,f);
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
#ifdef DEBUG
            printf("[Time: %.4f] [Master] Waiting for %d seconds...\n",TIME,seconds);
            fflush(stdout);
#endif
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
    sprintf(filename,"%s_par.out",client_id);

    FILE *file = fopen(filename,"a");

    if (file==NULL) {
        printf("Error opening output file %s\n",filename);
        return;
    }

    fprintf(file,"%s\n",actual_result);
    if (fclose(file)!=0) {
        printf("Error closing file %s\n",filename);
    }

#ifdef DEBUG
    printf("[Time: %.4f] [Master] Wrote result to %s\n",TIME,filename);
    fflush(stdout);
#endif
}

void master_process(int nProcesses, char *inputFileName) {

#ifdef DEBUG
    printf("[Time: %.4f] [Master] has started. Workers: %d.\n",TIME, nProcesses-1);
    fflush(stdout);
#endif

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
#ifdef DEBUG
            printf("[Time: %.4f] [Master] Sending command: %s to Worker %d\n",TIME,line,i);
            fflush(stdout);
#endif

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

#ifdef DEBUG
    printf("[Time: %.4f] [Master] All commands executed. Shutting down.\n",TIME);
    fflush(stdout);
#endif
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
#ifdef DEBUG
            printf("[Time: %.4f] [Worker %d] I received the stopping signal. Exiting!\n",TIME,rank);
            fflush(stdout);
#endif

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

#ifdef DEBUG
            printf("[Time: %.4f] [Worker %d] Finished %s. Sent %d bytes.\n",TIME,rank,command,offset);
            fflush(stdout);
#endif
        }
    }
    free(large_buffer);
}

void solve_serial(char *input) {

#ifdef DEBUG
    printf("[Serial] Started.\n");
#endif

    FILE *file=fopen(input,"r");
    if (file==NULL) {
        printf("Error opening file %s.\n",input);
        return;
    }

    char line[CMD_SIZE];
    while (get_next_command(file,line)) {
        char client_id[20],command[20],argument[50];
        sscanf(line,"%s %s %s",client_id,command,argument);

        char filename[50];
        sprintf(filename,"%s_ser.out",client_id);
        FILE *f_out=fopen(filename,"a");
        if (f_out==NULL) {
            printf("Error opening file %s.\n",filename);
            return;
        }

        if (strcmp(command,"PRIMES")==0) {
            fprintf(f_out,"%d\n",count_primes(atoi(argument)));
        }
        else if (strcmp(command,"PRIMEDIVISORS")==0) {
            fprintf(f_out,"%d\n",count_prime_divisors(atoi(argument)));
        }
        else if (strcmp(command,"ANAGRAMS")==0) {
            permute_for_serial(argument,0,strlen(argument)-1,f_out);
        }

        if (fclose(f_out)!=0) {
            printf("Error closing file %s.\n",filename);
            return;
        }
    }
    if (fclose(file)!=0) {
        printf("Error closing file.\n");
        return;
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

    MPI_Barrier(MPI_COMM_WORLD);
    START_TIME=MPI_Wtime();
    double start_par=START_TIME;

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

    MPI_Barrier(MPI_COMM_WORLD);
    double end_par=MPI_Wtime();
    double time_par=end_par-start_par;


    if (rank==0 && argc>=2) {
        printf("\n--- PERFORMANCE MEASUREMENTS ---\n");
        printf("Parallel time (%d processes): %f seconds\n",nProcesses,time_par);

        double start_ser=MPI_Wtime();
        solve_serial(argv[1]);
        double end_ser=MPI_Wtime();
        double time_ser=end_ser-start_ser;
        printf("Serial time: %f seconds\n",time_ser);

        double speedup=time_ser/time_par;
        printf("SPEEDUP: %f\n",speedup);
    }

    MPI_Finalize();
    return 0;
}