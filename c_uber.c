#include <stdio.h>
#include <stdlib.h>
#include <string.h>
//#include <mpi.h>
#include <sys/time.h>
#include <time.h>
#include <stdbool.h>   
#include <assert.h>
#include <omp.h>

int row_count=0;
typedef struct s_row{
    int date;
    int month;
    int year;

    int day;

    int hour;
    int min;
    
    float lat;
    float lon;
    char base[8];
} Row;

int numberLines(char* filename){
    FILE *fp;
    int count = 0;  // Line counter (result)
    char c;  // To store a character read from file
 
 
    // Open the file
    fp = fopen(filename, "r");
 
    // Check if file exists
    if (fp == NULL)
    {
        printf("Could not open file %s", filename);
        return 0;
    }
 
    // Extract characters from file and store in character c
    for (c = getc(fp); c != EOF; c = getc(fp))
        if (c == '\n') // Increment count if this character is newline
            count = count + 1;
 
    // Close the file
    fclose(fp);
    return count;
}

int getHour(int hr){
    if(hr == 0) return 23;
    else return hr-1;
}

void printall(Row* rows){
    for(int i=0; i<row_count; i++){
        printf("Min = %d\n", rows[i].min);
    }
}

char** str_split(char* a_str, const char a_delim)
{
    char** result    = 0;
    size_t count     = 0;
    char* tmp        = a_str;
    char* last_comma = 0;
    char delim[2];
    delim[0] = a_delim;
    delim[1] = 0;

    /* Count how many elements will be extracted. */
    while (*tmp)
    {
        if (a_delim == *tmp)
        {
            count++;
            last_comma = tmp;
        }
        tmp++;
    }

    /* Add space for trailing token. */
    count += last_comma < (a_str + strlen(a_str) - 1);

    /* Add space for terminating null string so caller
       knows where the list of returned strings ends. */
    count++;

    result = malloc(sizeof(char*) * count);

    if (result)
    {
        size_t idx  = 0;
        char* token = strtok(a_str, delim);

        while (token)
        {
            assert(idx < count);
            *(result + idx++) = strdup(token);
            token = strtok(0, delim);
        }
        assert(idx == count - 1);
        *(result + idx) = 0;
    }

    return result;
}

void addRow(Row *rows, Row r){
    strcpy(rows->base,r.base);
    rows->hour = r.hour;
    rows->min = r.min;
    rows->day = r.day;
    rows->lat = r.lat;
    rows->lon = r.lon;
    rows->month = r.month;
    rows->year = r.year;
    rows->date = r.date;
}

int readCSV(FILE *stream, Row* rows, int linec){
    char line[1024];
    if(stream == NULL){
        printf("Oh no!\n");
        return -1;
    }
    bool first = true;
    //omp_lock_t writelock;

    //omp_init_lock(&writelock);

    //while (fgets(line, 1024, stream))
    //#pragma omp parallel for
    for(int k = 0;k<linec; k++)
    {
        if(!fgets(line, 1024, stream)) continue;
        if(first){first=false; continue;}

        //omp_set_lock(&writelock);
        char* tmp = strdup(line);
        //omp_unset_lock(&writelock);
        //Get all columns here
        char** tokens = str_split(tmp, ',');
        char *dt;
        float lon, lat;
        char *base;

        if(!tokens) continue;

        dt = *tokens;
        lat = atof(*(tokens + 1));
        lon = atof(*(tokens + 2));
        base = *(tokens+3);
        base[strcspn(base, "\n")] = '\0';

        Row row;

        //Sanity check
        //printf("Long = %f\tLat = %f\tBase = %s\n", lon, lat, base);
        //printf("Long = %f\tLat = %f\tBase = %s\n", row.lon, row.lat, row.base);
        //

        //Date Parsing
        struct tm tm;
        time_t t;
        memset((void *) &tm, 0, sizeof(tm));
        //char* dt = getfield(tmp, 1);
        if (strptime(dt, "\"%m/%d/%Y %H:%M:%S\"", &tm) == NULL){
            printf("Error parsing DateTime\n");
            //break;
        }
        else{
            t = mktime(&tm);
            if(t>=0){
                // Assign to struct
                row.month = localtime(&t) -> tm_mon;
                row.date = localtime(&t) -> tm_mday;
                row.hour = getHour(localtime(&t) -> tm_hour);
                row.year = localtime(&t) -> tm_year;
                row.min = localtime(&t) -> tm_min;

                row.lat = lat;
                row.lon = lon;
                strcpy(row.base,base);
                //
            }
        }

        //Free all
        free(*tokens);
        free(*(tokens+1));
        free(*(tokens+2));
        free(*(tokens+3));
        free(tokens);
        free(tmp);

        //Sanity check
        //printf("Hour = %d\tMin = %d\tBase = %s\n", row.hour, row.min, row.base);
        //

        //Add to the struct array
        row_count++;
        //printf("Number of rows = %d\n", row_count);
        rows = (Row*) realloc(rows, sizeof(Row) * row_count);
        //printf("Check if realloc\n");
        rows[row_count-1] = row;
        //addRow(&rows[row_count-1], row);
        //printf("%p\n", *(rows+row_count-1));
        //printf("Hour= %d\n", rows[row_count-1].hour);
        //
    }
    //omp_destroy_lock(&writelock);
    //printall(rows);
    return 0;
}

int main(int argc, char *argv[])
{
    //int threads = atoi(argv[1]);
    struct timeval t1, t2;
    gettimeofday(&t1, NULL);
    
    FILE* stream[6];
    
    stream[0] = fopen("/home/ishan/workbox/ml_uber/uber-raw-data-apr14.csv", "r");
    int n0 = numberLines("/home/ishan/workbox/ml_uber/uber-raw-data-apr14.csv");

    stream[1] = fopen("/home/ishan/workbox/ml_uber/uber-raw-data-may14.csv", "r");
    int n1 = numberLines("/home/ishan/workbox/ml_uber/uber-raw-data-may14.csv");
    
    stream[2] = fopen("/home/ishan/workbox/ml_uber/uber-raw-data-jun14.csv", "r");
    int n2 = numberLines("/home/ishan/workbox/ml_uber/uber-raw-data-jun14.csv");
    
    stream[3] = fopen("/home/ishan/workbox/ml_uber/uber-raw-data-jul14.csv", "r");
    int n3 = numberLines("/home/ishan/workbox/ml_uber/uber-raw-data-jul14.csv");
    
    stream[4] = fopen("/home/ishan/workbox/ml_uber/uber-raw-data-aug14.csv", "r");
    int n4 = numberLines("/home/ishan/workbox/ml_uber/uber-raw-data-aug14.csv");
    
    stream[5] = fopen("/home/ishan/workbox/ml_uber/uber-raw-data-sep14.csv", "r");
    int n5 = numberLines("/home/ishan/workbox/ml_uber/uber-raw-data-sep14.csv");
    
    /*
    MPI_Init(NULL,NULL);
    int pid;
    int size;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    */
    Row* rows = (Row*)malloc(sizeof(Row) * row_count);
    readCSV(stream[0], rows, n0);
    //printf("%d\n", rows[23].hour);
    //printall(rows);
    //readCSV(stream[1], rows, n1);
    //readCSV(stream[2], rows, n2);
    //readCSV(stream[3], rows, n3);
    //readCSV(stream[4], rows, n4);
    //readCSV(stream[5], rows, n5);
    
    /*
    readCSV(stream[pid]);

    MPI_Finalize();
    */
   gettimeofday(&t2, NULL);
   double elapsedTime = (t2.tv_sec - t1.tv_sec) * 1000.0;      // sec to ms
   elapsedTime += (t2.tv_usec - t1.tv_usec) / 1000.0; 
   printf("%f ms.\n", elapsedTime);
   //free(rows);
   return 0;
}