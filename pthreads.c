#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

//tuple structure
struct tuple {
    char userId[5];
    char action;
    char topic[16];
    int score;
    int rid;
};

int producerId = 0;
int listOfTuples = 0;
int status = 0;
int numOfReducers;
int numOfSlots;
int buffer[100];
FILE *fp;

//Declaring pthreads
pthread_mutex_t lock;
pthread_cond_t conditionFull;
pthread_cond_t conditonEmpty;

struct ids {
    char userId[5];
    int rid;
};

struct tuple tuples[100];
struct ids idList[100];

//Producer - Generates data and puts it into the buffer
void *mapper(void *param) {
    int regex;
    int n = 0, flag = 0;
    char temp;
    while (1) {
        pthread_mutex_lock(&lock);

        //read the tuple from file line by line
        regex = scanf("(%[^,],%c,%[^)])%c", tuples[producerId].userId, &tuples[producerId].action,
                      tuples[producerId].topic, &temp);
        if (regex != 4) {
            status = 1;
            pthread_cond_broadcast(&conditonEmpty);
            pthread_cond_broadcast(&conditionFull);
            printf("mapper signalled done!\n");
            pthread_mutex_unlock(&lock);
            break;
        }

        //Maps the actions to a score
        switch (tuples[producerId].action) {
            case 'P':
                tuples[producerId].score = 50;
                break;
            case 'L':
                tuples[producerId].score = 20;
                break;
            case 'D':
                tuples[producerId].score = -10;
                break;
            case 'C':
                tuples[producerId].score = 30;
                break;
            case 'S':
                tuples[producerId].score = 40;
                break;
        }

      //insert the userId and rid to the id list which is used for reducer and set the rid of every tuple
        if (producerId == 0) {
            strcpy(idList[listOfTuples].userId, tuples[producerId].userId);
            idList[listOfTuples].rid = 0;
            tuples[producerId].rid = idList[listOfTuples].rid;
        } else {
            for (n = 0; n <= listOfTuples; n++) {
                if (strcmp(idList[n].userId, tuples[producerId].userId) == 0) {
                    tuples[producerId].rid = idList[n].rid;
                    flag = 1;
                }
            }
            if (flag == 0) {
                listOfTuples++;
                strcpy(idList[listOfTuples].userId, tuples[producerId].userId);
                idList[listOfTuples].rid = idList[listOfTuples - 1].rid + 1;
                tuples[producerId].rid = idList[listOfTuples].rid;
            }
            flag = 0;

        }

        while (buffer[tuples[producerId].rid] == numOfSlots) {
            printf("mapper waiting on full\n");
            pthread_cond_wait(&conditionFull, &lock);
        }
        printf("mapper insert tuple\n");
        printf("(%s,%s,%d),%d\n", tuples[producerId].userId, tuples[producerId].topic, tuples[producerId].score,
               tuples[producerId].rid);
        buffer[tuples[producerId].rid]++;
        producerId++;
        printf("Rid: %d, TupleProd: %d, BUffer: %d\n", tuples[producerId - 1].rid, producerId,
               buffer[tuples[producerId - 1].rid]);
        pthread_cond_signal(&conditonEmpty);
        pthread_mutex_unlock(&lock);
    }
    return param;
}

//Consumer - Consumes data one tuple at a time
void *reducer(void *param) {
    long rid = (long) param;
    int n = 0, s = 0;
    //array of consumed tuple
    struct tuple consumerBuffer[100];
    int tupleCond = 0;
    int i = 0, j = 0, k = 0, flag = 0;
    while (1) {
        pthread_mutex_lock(&lock);
        while (buffer[rid] == 0 && !status) {
            printf("reducer %ld waiting on empty\n", rid);
            pthread_cond_wait(&conditonEmpty, &lock);
        }

        if (buffer[rid] == 0 && status) {
            struct tuple reducer[100];
            printf("reducer %ld is exiting\n", rid);
            printf("\n");

            for (i = 0; i <= tupleCond; i++) {
                if (i == 0) {
                    strcpy(reducer[j].userId, consumerBuffer[i].userId);
                    reducer[j].score = consumerBuffer[i].score;
                    strcpy(reducer[j].topic, consumerBuffer[i].topic);
                } else {
                    for (k = 0; k <= j; k++) {
                        if (strcmp(consumerBuffer[i].topic, reducer[k].topic) == 0) {
                            reducer[k].score += consumerBuffer[i].score;
                            flag = 1;
                        }
                    }
                    if (flag == 0) {
                        j++;
                        strcpy(reducer[j].userId, consumerBuffer[i].userId);
                        reducer[j].score = consumerBuffer[i].score;
                        strcpy(reducer[j].topic, consumerBuffer[i].topic);
                    }
                    flag = 0;
                }
            }

            //the process of exiting a reducer and print its output
            for (i = 0; i < j; i++) {
                printf("(%s,%s,%d)\n", reducer[i].userId, reducer[i].topic, reducer[i].score);
                fprintf(fp, "(%s,%s,%d)\n", reducer[i].userId, reducer[i].topic, reducer[i].score);
            }
            printf("\n");
            pthread_mutex_unlock(&lock);
            break;
        }

        //consume the tuple by linearly searching the tuples produced by mapper
        for (n = s; n <= producerId; n++) {
            if (tuples[n].rid == rid) {
                s = n + 1;
                strcpy(consumerBuffer[tupleCond].userId, tuples[n].userId);
                consumerBuffer[tupleCond].score = tuples[n].score;
                strcpy(consumerBuffer[tupleCond].topic, tuples[n].topic);
                printf("(%s,%s,%d)\n", consumerBuffer[tupleCond].userId, consumerBuffer[tupleCond].topic,
                       consumerBuffer[tupleCond].score);
                tupleCond++;
                buffer[rid]--;
                printf("reducer: %ld TupleCond: %d Buffer: %d\n", rid, tupleCond, buffer[rid]);
                break;
            }
        }
        pthread_cond_signal(&conditionFull);
        pthread_mutex_unlock(&lock);
    }
    return param;
}

int main(int argc, char *argv[]) {
    long i = 0;
    numOfSlots = atoi(argv[1]);
    numOfReducers = atoi(argv[2]);

    //initializing the buffer
    for (i = 0; i < numOfReducers; i++) {
        buffer[i] = 0;
    }

    fp = fopen("output.txt", "w+");

    pthread_t threadID[numOfReducers + 1];
    void *threadStatus;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_mutex_init(&lock, NULL);
    pthread_cond_init(&conditionFull, NULL);
    pthread_cond_init(&conditonEmpty, NULL);

    //creating the mapper thread
    pthread_create(&threadID[0], &attr, mapper, (void *) i);

    //creating the reducer threads
    for (i = 0; i < numOfReducers; i++) {
        pthread_create(&threadID[i + 1], &attr, reducer, (void *) i);
    }

    //Ending the mapper thread
    pthread_join(threadID[0], &threadStatus);
    printf("Mapper joined by Main\n");

    //Ending the reducer threads
    for (i = 0; i < numOfReducers; i++) {
        pthread_join(threadID[i + 1], &threadStatus);
        printf("Reducer %ld joined by Main\n", i);
    }

    pthread_mutex_destroy(&lock);
    pthread_cond_destroy(&conditionFull);
    pthread_cond_destroy(&conditonEmpty);
    pthread_attr_destroy(&attr);
    fclose(fp);

    pthread_exit(NULL);
}
