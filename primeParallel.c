//  primeParallel
//  Created by Pandora on 2023/3/17.
//  NEED AT LEAST 93GB MEMORY!

#include <stdio.h>
#include <stdbool.h>
#include <math.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
//  CAN CHANGE FROM 1 TO 78498, BUT 16 TO 32 IS RECOMMENDED.
#define threadNumber0 32
//  DO NOT CHANGE!
#define threadNumber1 59
//  CAN CHANGE FROM 1 TO 59, IF SET TO 59, 176GB MEMORY IS REQUIRED!
#define threadNumber2 8

bool storeIntoBinary=true;

typedef struct previousPrimeStruct{
    long long unsigned int indexArray;
    long long unsigned int indexLLU;
    long long unsigned int indexBit;
    long long unsigned int thisPrime;
}PRS;

typedef struct readFromBitsStruct{
    long long unsigned int primeCount;
    long long unsigned int theLastPrime;
}RFBS;

long long unsigned int *primArray[threadNumber1]={0};
pthread_mutex_t mutex1;
pthread_mutex_t mutex2;
pthread_mutex_t mutex3;
sem_t *semaphorePrimArray[threadNumber1]={0};
int threadPrimeID;
PRS *readFromBitsStruct;
long long unsigned int primArrayIndex;
bool getMyThreadID;
bool getMyIndex;
RFBS *RFBS_Threads;
long long unsigned int primeCountGlobal[threadNumber1]={0};
long long unsigned int firstPrimeGlobal[threadNumber1]={0};
long long unsigned int lastPrimeGlobal[threadNumber1]={0};
bool threadPrimeJoined=false;

long long unsigned isPrime(long long unsigned int bitsInformation,long long unsigned int indexArray,long long unsigned int indexLLU,char *fiveBytesUnsigned,long long unsigned indexFBU){
    //  fiveBytesUnsigned FBU
    int indexBit=0;
    long long unsigned int thisPrime=0;
    while(((((indexLLU<<7)+(indexBit<<1))^(indexArray<<34))+1)<1000000000000LLU){
        if(((*(primArray[indexArray]+indexLLU))&(1LLU<<(64-indexBit-1)))){
            thisPrime=(((indexLLU<<7)+(indexBit<<1))^(indexArray<<34))+1;
            fiveBytesUnsigned[indexFBU+0]=(thisPrime>>32)&255;
            fiveBytesUnsigned[indexFBU+1]=(thisPrime>>24)&255;
            fiveBytesUnsigned[indexFBU+2]=(thisPrime>>16)&255;
            fiveBytesUnsigned[indexFBU+3]=(thisPrime>> 8)&255;
            fiveBytesUnsigned[indexFBU+4]=(thisPrime>> 0)&255;
            indexFBU+=5;
        }
        indexBit++;
        if(indexBit>63){
            break;
        }
    }
    return indexFBU;
}

long long unsigned int getNextPrime(int ThreadID_Function){
    long long unsigned int preciousPrimeF1=(readFromBitsStruct->thisPrime)-1;
    long long unsigned int indexArray_inner=preciousPrimeF1>>34;
    long long unsigned int indexLLU_inner=(preciousPrimeF1&((1LLU<<34)-1))>>7;
    long long unsigned int indexBit_inner=((preciousPrimeF1&127)>>1)+1;
    while(!((*(primArray[indexArray_inner]+indexLLU_inner))&(1LLU<<(64-indexBit_inner-1)))){
        indexBit_inner++;
        if(indexBit_inner>63){
            indexBit_inner=0;
            indexLLU_inner++;
        }
        if(indexLLU_inner>=(1LLU<<27)){
            indexBit_inner=0;
            indexLLU_inner=0;
            indexArray_inner++;
        }
    }
    long long unsigned int thisPrime=(((indexLLU_inner<<7)+(indexBit_inner<<1))^(indexArray_inner<<34))+1;
    readFromBitsStruct->indexArray=indexArray_inner;
    readFromBitsStruct->indexLLU=indexLLU_inner;
    readFromBitsStruct->indexBit=indexBit_inner;
    readFromBitsStruct->thisPrime=thisPrime;
    printf("I AM THREAD %d, I AM PROCESSING ON FACTOR %llu\n",ThreadID_Function,thisPrime);
    return thisPrime;
}

void getOut(long long unsigned int toMove){
    long long unsigned int toMoveF1=toMove-1;
    long long unsigned int indexArray=toMoveF1>>34;
    long long unsigned int indexLLU=(toMoveF1&((1LLU<<34)-1))>>7;
    long long unsigned int indexBit=((toMoveF1&127)>>1);
    long long unsigned int indexsemaphore=indexLLU>>3;
    sem_wait((semaphorePrimArray[indexArray]+indexsemaphore));
    *(primArray[indexArray]+indexLLU)=(*(primArray[indexArray]+indexLLU))&(~(1LLU<<(64-indexBit-1)));
    sem_post((semaphorePrimArray[indexArray]+indexsemaphore));
}

void *threadMalloc(){
    long long unsigned int indexArray=primArrayIndex;
    getMyIndex=true;
    primArray[indexArray]=(long long unsigned int*)malloc(1LLU<<30);
    memset(primArray[indexArray],255,1<<30);
    semaphorePrimArray[indexArray]=(sem_t*)malloc(1LLU<<29);
    for(unsigned semaphoreIndex=0;semaphoreIndex<(1LLU<<24);semaphoreIndex++){
        int ifNotSuccess=1;
        unsigned tryCount=0;
        while(ifNotSuccess){
            ifNotSuccess=sem_init((semaphorePrimArray[indexArray]+semaphoreIndex),0,1);
            tryCount++;
            if(tryCount==1048576){
                printf("UNABLE TO INITALIZE SEMAPHORE FOR indexArray: %llu AT indexLLU: %u\n",indexArray,semaphoreIndex);
                abort();
            }
        }
    }
    return NULL;
}

void *threadPrime(){
    long long unsigned int product=3;
    long long unsigned int toMove;
    long long unsigned int primeThread=3;
    int trueThreadID=threadPrimeID;
    getMyThreadID=true;
    while(primeThread<1000000LLU){
        pthread_mutex_lock(&mutex1);
        primeThread=getNextPrime(trueThreadID);
        pthread_mutex_unlock(&mutex1);
        toMove=3*product;
        while(toMove<1000000000000LLU){
            getOut(toMove);
            product+=2;
            toMove=primeThread*product;
        }
        product=3;
    }
    return NULL;
}

void *threadReadFromBits(){
    long long unsigned int indexArray=primArrayIndex;
    getMyIndex=true;
    for(int semaphoreIndex=0;semaphoreIndex<(1LLU<<27);semaphoreIndex++){
        sem_destroy((semaphorePrimArray[indexArray]+semaphoreIndex));
    }
    free(semaphorePrimArray[indexArray]);
    long long unsigned int indexLLU=0;
    int indexBit=0;
    long long unsigned int thisPrime=0;
    long long unsigned int priemCount_inner=0;
    long long unsigned int firstPrime=0;
    long long unsigned int lastPrime=0;
    bool isFirstPrime=true;
    while(((((indexLLU<<7)+(indexBit<<1))^(indexArray<<34))+1)<1000000000000LLU){
        if(((*(primArray[indexArray]+indexLLU))&(1LLU<<(64-indexBit-1)))){
            priemCount_inner++;
            thisPrime=(((indexLLU<<7)+(indexBit<<1))^(indexArray<<34))+1;
            if(isFirstPrime){
                firstPrime=thisPrime;
                isFirstPrime=false;
            }
        }
        indexBit++;
        if(indexBit>63){
            indexBit=0;
            indexLLU++;
            if(indexLLU==(1LLU<<27)){
                break;
            }
        }
    }
    pthread_mutex_lock(&mutex2);
    RFBS_Threads->primeCount=RFBS_Threads->primeCount+priemCount_inner;
    primeCountGlobal[indexArray]=priemCount_inner;
    firstPrimeGlobal[indexArray]=firstPrime;
    lastPrimeGlobal[indexArray]=thisPrime;
    printf("INDEX ARRAY: %llu, INDEX LLU: %llu, LAST PRIME: %llu, priemCount_inner: %llu\n",indexArray,indexLLU,thisPrime,priemCount_inner);
    if(thisPrime>(RFBS_Threads->theLastPrime)){
        RFBS_Threads->theLastPrime=thisPrime;
    }
    pthread_mutex_unlock(&mutex2);
    return NULL;
}

void *threadStore(){
    for(unsigned indexArray=0;indexArray<threadNumber1;indexArray++){
        long long unsigned int priemCount_inner=0;
        pthread_mutex_lock(&mutex3);
        if(primeCountGlobal[indexArray]){
            priemCount_inner=primeCountGlobal[indexArray];
            primeCountGlobal[indexArray]=0;
        }
        pthread_mutex_unlock(&mutex3);
        if(!(priemCount_inner)){
            continue;
        }
        long long unsigned int indexFBU=0;
        char *fiveBytesUnsigned=(char*)malloc(priemCount_inner*5);
        printf("BEGIN TO WRITE FILE %u\n",indexArray);
        for(long long unsigned int indexLLU=0;indexLLU<(1<<27);indexLLU++){
            indexFBU=isPrime(*(primArray[indexArray]+indexLLU),indexArray,indexLLU,fiveBytesUnsigned,indexFBU);
        }
        if(indexArray==0){
            fiveBytesUnsigned[0]=2;
        }
        unsigned fileNameLength=0;
        // FILE NAME EXAMPLE:
        // LENGTH_IN_BYTES_455052511__FROM_704374636463_TO_721554505723
        // 123456789ABCDEFG         1234567            1234
        // F=16, G=17
        fileNameLength=17+(unsigned)log10(priemCount_inner*5)+1+7+(unsigned)log10(firstPrimeGlobal[indexArray])+1+4+(unsigned)log10(lastPrimeGlobal[indexArray])+1+1;
        char *fileName;
        fileName=(char*)malloc(fileNameLength);
        memset(fileName,0,fileNameLength);
        strcat(fileName,"LENGTH_IN_BYTES_");
        char *BYTES_Char=(char*)malloc((unsigned)log10(priemCount_inner*5)+1+1);
        sprintf(BYTES_Char,"%llu",priemCount_inner*5);
        strcat(fileName,BYTES_Char);
        strcat(fileName,"__FROM_");
        char *FROM_Char=(char*)malloc((unsigned)log10(firstPrimeGlobal[indexArray])+1+1);
        sprintf(FROM_Char,"%llu",firstPrimeGlobal[indexArray]);
        strcat(fileName,FROM_Char);
        strcat(fileName,"_TO_");
        char *TO_Char=(char*)malloc((unsigned)log10(lastPrimeGlobal[indexArray])+1+1);
        sprintf(TO_Char,"%llu",lastPrimeGlobal[indexArray]);
        strcat(fileName,TO_Char);
        fileName[fileNameLength-1]='\0';
        FILE *binaryFile=NULL;
        short openFileTryTime=0;
        bool ifContinue=false;
        while(!(binaryFile)){
            binaryFile=fopen(fileName,"wb");
            openFileTryTime++;
            if(openFileTryTime==5){
                printf("STORE PRIMES TO FILE FOR indexArray %u FAILED\n",indexArray);
                ifContinue=true;
                free(fiveBytesUnsigned);
                free(fileName);
                free(BYTES_Char);
                free(FROM_Char);
                free(TO_Char);
            }
        }
        if(ifContinue){
            continue;
        }
        fwrite(fiveBytesUnsigned,sizeof(char),priemCount_inner*5,binaryFile);
        free(fiveBytesUnsigned);
        free(fileName);
        free(BYTES_Char);
        free(FROM_Char);
        free(TO_Char);
    }
    return NULL;
}

void getFactors(){
    long long unsigned int product=3;
    long long unsigned int toMove=3*product;
    long long unsigned int thisPrime=3;
    while(thisPrime<1000LLU){
        thisPrime=getNextPrime(-1);
        toMove=thisPrime*product;
        while(toMove<1000000LLU){
            toMove=thisPrime*product;
            getOut(toMove);
            product+=2;
        }
        product=3;
    }
}

int main(int argc, const char * argv[]) {
    time_t start,end;
    start=time(NULL);
    pthread_t threadPoolPrime[threadNumber0];
    pthread_t threadPoolMalloc[threadNumber1];
    pthread_t threadPoolRFB[threadNumber1];
    pthread_t threadPoolStore[threadNumber2];
    readFromBitsStruct=malloc(sizeof(PRS));
    readFromBitsStruct->indexArray=0;
    readFromBitsStruct->indexLLU=0;
    readFromBitsStruct->indexBit=0;
    readFromBitsStruct->thisPrime=2;
    primArrayIndex=0;
    for(int threadIndex=0;threadIndex<threadNumber1;threadIndex++){
        getMyIndex=false;
        pthread_t currentThread;
        pthread_create(&currentThread,NULL,threadMalloc,NULL);
        threadPoolMalloc[threadIndex]=currentThread;
        usleep(1000);
        while(!getMyIndex){
        }
        primArrayIndex++;
    }
    for(int threadIndex=0;threadIndex<threadNumber1;threadIndex++){
        pthread_join(threadPoolMalloc[threadIndex],NULL);
    }
    getFactors();
    threadPrimeID=0;
    readFromBitsStruct->indexArray=0;
    readFromBitsStruct->indexLLU=0;
    readFromBitsStruct->indexBit=0;
    readFromBitsStruct->thisPrime=2;
    pthread_mutex_init(&mutex1,NULL);
    for(int threadIndex=0;threadIndex<threadNumber0;threadIndex++){
        getMyThreadID=false;
        pthread_t currentThread;
        pthread_create(&currentThread,NULL,threadPrime,NULL);
        threadPoolPrime[threadIndex]=currentThread;
        usleep(1000);
        while(!getMyThreadID){
        }
        threadPrimeID++;
    }
    for(int threadIndex=0;threadIndex<threadNumber0;threadIndex++){
        pthread_join(threadPoolPrime[threadIndex],NULL);
    }
    threadPrimeJoined=true;
    primArrayIndex=0;
    RFBS_Threads=malloc(sizeof(RFBS));
    RFBS_Threads->primeCount=0;
    RFBS_Threads->theLastPrime=0;
    pthread_mutex_init(&mutex2,NULL);
    for(int threadIndex=0;threadIndex<threadNumber1;threadIndex++){
        getMyIndex=false;
        pthread_t currentThread;
        pthread_create(&currentThread,NULL,threadReadFromBits,NULL);
        threadPoolRFB[threadIndex]=currentThread;
        usleep(1000);
        while(!getMyIndex){
        }
        primArrayIndex++;
    }
    for(int threadIndex=0;threadIndex<threadNumber1;threadIndex++){
        pthread_join(threadPoolRFB[threadIndex],NULL);
    }
    pthread_mutex_destroy(&mutex1);
    pthread_mutex_destroy(&mutex2);
    printf("CALCULATION DONE!\nPRIME COUNT: %llu, THE LAST PRIME: %llu\n",RFBS_Threads->primeCount,RFBS_Threads->theLastPrime);
    end=time(NULL);
    printf("TIME CONSUMPTION %ld SECOND\n",end-start);
    if(!storeIntoBinary){
        printf("SET NOT TO WRITE BINARY FILES.\n");
        return 0;
    }
    pthread_mutex_init(&mutex3,NULL);
    for(int threadIndex=0;threadIndex<threadNumber2;threadIndex++){
        pthread_t currentThread;
        pthread_create(&currentThread,NULL,threadStore,NULL);
        threadPoolStore[threadIndex]=currentThread;
        usleep(1000);
    }
    for(int threadIndex=0;threadIndex<threadNumber2;threadIndex++){
        pthread_join(threadPoolStore[threadIndex],NULL);
    }
    pthread_mutex_destroy(&mutex3);
    return 0;
}
