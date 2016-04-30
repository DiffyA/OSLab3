#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "../include/concurrency_layer.h"

pthread_mutex_t fullQueueMutex;
pthread_mutex_t globalWrite;
pthread_cond_t fullQueue;

void init_concurrency_mechanisms() {
    pthread_mutex_init(&fullQueueMutex, NULL);
    pthread_mutex_init(&globalWrite, NULL);
    pthread_cond_init(&fullQueue, NULL);
}

void destroy_concurrency_mechanisms() {
}

// Broker: inserts operations obtained from a batch file into the operations_queue of a given market.
void * broker(void * args) {
/*Input: broker_info --> 
1) char batch_file[256]
2) stock_market * market)
*/
/*    pthread_mutex_lock(&fullQueueMutex);
    printf("ENTER BROKER \n");
    uleep(500000);
    pthread_mutex_unlock(&full_queue_mutex);
*/
	usleep(5000);
    broker_info * currentInfo = (broker_info*) args; // Cast the *void pointer to a *broker_info pointer. 
    stock_market * currentMarket = currentInfo->market; // Create a stock_market pointer. This might not be necessary but handy in the enqueue_operation call.
    operation currentOperation; // operation struct to hold information of the operation on current batch line.
    iterator * brokerIterator = new_iterator(currentInfo->batch_file); // iterator that will parse each line of the batch file.

    if (brokerIterator == NULL) { // Error creating a new iterator. 
        perror("Error creating a new iterator. \n"); 
        return; // void calls return no value. This might show up as a warning however.
    }
    else { // Iterator was created succesfully
    	while (next_operation(brokerIterator, currentOperation.id, &currentOperation.type, &currentOperation.num_shares, &currentOperation.share_price) != EOF) {  // While there are still lines to read, continue.
		pthread_mutex_lock(&globalWrite);
		enqueue_operation(currentMarket->stock_operations, &currentOperation); // Enqueues the operation created by the next_operation call.
		printf("CURRENT ID: %d \n", pthread_self());
		usleep(500000);
		while (operations_queue_full(currentMarket->stock_operations) == 1) {
			printf("QUEUE FULL! WAITING! \n");
			pthread_cond_wait(&fullQueue, &globalWrite);
		}
		pthread_mutex_unlock(&globalWrite);
		usleep(100000);
	}
	
	destroy_iterator(brokerIterator); // Destroys iterator to free resources.
    }
}


void* operation_executer(void * args) {
/* Input: exec_info -->
1) int *exit
2) stock_market * market
3) pthread_mutex_t * exit_mutex
*/
    printf("ENTER OP_EXEC \n");
    exec_info * currentInfo = (exec_info*) args;
    stock_market * currentMarket = currentInfo->market;
    operation currentOperation;

    while (*(currentInfo->exit) != 1) { // While exit flag is not active
	pthread_mutex_lock(&globalWrite);
	if (operations_queue_empty(currentMarket->stock_operations) == 0) { // If queue is not empty
	    dequeue_operation(currentMarket->stock_operations , &currentOperation);
	    pthread_cond_signal(&fullQueue); // Queue is no longer full as one operation has been dequeued.
	    process_operation(currentInfo->market, &currentOperation);
	}

	pthread_mutex_unlock(&globalWrite);
    }
    return;
}

void * stats_reader(void * args){ 
/*Input: reader_info --> 
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
