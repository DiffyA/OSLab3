#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "../include/concurrency_layer.h"

pthread_mutex_t fullQueueMutex;
pthread_mutex_t globalWrite;
pthread_cond_t fullQueue;
pthread_cond_t emptyQueue;

void init_concurrency_mechanisms() {
    pthread_mutex_init(&fullQueueMutex, NULL);
    pthread_mutex_init(&globalWrite, NULL);
    pthread_cond_init(&fullQueue, NULL);
    pthread_cond_init(&emptyQueue, NULL);
}

void destroy_concurrency_mechanisms() {
}

// Broker: inserts operations obtained from a batch file into the operations_queue of a given market.
void * broker(void * args) {
/*Input: broker_info —> 
1) char batch_file[256]
2) stock_market * market)
*/
    broker_info * currentInfo = (broker_info*) args; // Cast the *void pointer to a *broker_info pointer. 
    stock_market * currentMarket = currentInfo->market; // Create a stock_market pointer. This might not be necessary but handy in the enqueue_operation call.
    operation currentOperation; // operation struct to hold information of the operation on current batch line.
    iterator * brokerIterator = new_iterator(currentInfo->batch_file); // iterator that will parse each line of the batch file.

    if (brokerIterator == NULL) { // Error creating a new iterator. 
        perror("Error creating a new iterator. \n"); 
        pthread_exit(NULL);
    }
    else { // Iterator was created succesfully
        pthread_mutex_lock(&globalWrite);
        while (next_operation(brokerIterator, currentOperation.id, &currentOperation.type, &currentOperation.num_shares, &currentOperation.share_price) != EOF) {  // While there are still lines to read, continue.
            while (operations_queue_full(currentMarket->stock_operations) == 1) { // When the queue is full
                pthread_cond_wait(&fullQueue, &globalWrite); // Wait until you get the signal it is no longer full. 
            }
            enqueue_operation(currentMarket->stock_operations, &currentOperation); // Enqueues the operation created by the next_operation call.
            pthread_cond_signal(&emptyQueue); // Signal to the executer that the queue is no longer empty.
        }
        pthread_mutex_unlock(&globalWrite);
        destroy_iterator(brokerIterator); // Destroys iterator to free resources.
    }
    pthread_exit(NULL);
}


void* operation_executer(void * args) {
/* Input: exec_info —>
1) int *exit
2) stock_market * market
3) pthread_mutex_t * exit_mutex
*/
    exec_info * currentInfo = (exec_info*) args;
    stock_market * currentMarket = currentInfo->market;
    operation currentOperation;

    while(1){ // Will do continuously until no more operations in the queue and exit flag is on.
        pthread_mutex_lock(&globalWrite); // We will access and modify the queue, so lock it.
        if (operations_queue_empty(currentMarket->stock_operations) == 0) { // If queue is not empty
            dequeue_operation(currentMarket->stock_operations , &currentOperation); // Dequeue the operation.
            pthread_cond_signal(&fullQueue); // Send signal that queue is no longer full as one operation has been dequeued.
            process_operation(currentInfo->market, &currentOperation); // Process the dequeued operation.
        }
        else { //Queue is empty
            pthread_mutex_lock((currentInfo->exit_mutex)); // Lock the exit mutex as we are about to access the exit variable.
            if (*(currentInfo->exit)) { // Exit is active: we are not waiting for more brokers.
                pthread_mutex_unlock((currentInfo->exit_mutex)); // Unlock exit mutex.
                pthread_exit(NULL); // Thread exits.
            }
            else { // If exit is not active: we are waiting for more brokers' operations.
                pthread_cond_wait(&emptyQueue, &globalWrite); // Wait for a signal from the variable, unlock the globalWrite so a broker may use CPU                                
            }
            pthread_mutex_unlock((currentInfo->exit_mutex));
        }
        pthread_mutex_unlock(&globalWrite); // Unlock the globalWrite.
    }
}

void * stats_reader(void * args){ 
/*Input: reader_info —> 
1) int * exit
2) stock_market * market
3) pthread_mutex_t * exit_mutex
4) unsigned int frequency
*/
      printf("STAT READER \n");
      reader_info * currentInfo  = (reader_info*) args;
      stock_market * currentMarket = currentInfo->market;

     // while(*(currentInfo->exit) != 1){

      update_market_statistics(currentInfo->market);
      usleep(currentInfo->frequency);
}
