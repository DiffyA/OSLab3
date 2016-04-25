#include "include/concurrency_layer.h"

int main(int argc, char * argv[]){
	pthread_t tid[2];
	stock_market market_madrid;
	int exit = 0;
	pthread_mutex_t exit_mutex;

	// Init market and concurrency mechanisms
	init_market(&market_madrid, "stocks.txt");
	init_concurrency_mechanisms();
	pthread_mutex_init(&exit_mutex,NULL);
	
	// Init broker_info structure for the broker thread
	broker_info info_b1;
	strcpy(info_b1.batch_file, "batch_operations.txt");
	info_b1.market = &market_madrid;
	
	// Init exec_info structure for the operation_executer thread
	exec_info info_ex1;
	info_ex1.market = &market_madrid;
	info_ex1.exit = &exit;
	info_ex1.exit_mutex = &exit_mutex;
	
	// Init reader_info for the stats_reader thread
	//reader_info info_re1;
	//info_re1.market = &market_madrid;
	//info_re1.exit = &exit;
	//info_re1.exit_mutex = &exit_mutex;
	//info_re1.frequency = 100000;
	
	// Create broker and exec threads
	pthread_create(&(tid[0]), NULL, &broker, (void*) &info_b1);
	pthread_create(&(tid[1]), NULL, &operation_executer, (void*) &info_ex1);
	
	// Join broker threads
	void * res;
	pthread_join(tid[0],&res);
	
	// Put exit flag = 1 after brokers completion
	pthread_mutex_lock(&exit_mutex);
	exit = 1;
	pthread_mutex_unlock(&exit_mutex);
	
	// Join the rest of the threads
	pthread_join(tid[1],&res);
	
	// Print final statistics of the market
	print_market_status(&market_madrid);
	
	// Destroy market and concurrency mechanisms
	delete_market(&market_madrid);
	destroy_concurrency_mechanisms();
	pthread_mutex_destroy(&exit_mutex);
  
	return 0;
}