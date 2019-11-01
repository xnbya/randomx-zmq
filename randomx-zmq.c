#include <czmq.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <sys/time.h>

#include "RandomX/src/randomx.h"

//#define DEBUG 1

#define S_NOTIFY_MSG " "
#define S_ERROR_MSG "Error writing to pipe when exiting\n"
static int s_fd;
static void s_signal_handler (int signal_value)
{
    int rc = write(s_fd, S_NOTIFY_MSG, sizeof(S_NOTIFY_MSG));
    if(rc != sizeof(S_NOTIFY_MSG)) {
        write(STDOUT_FILENO, S_ERROR_MSG, sizeof(S_ERROR_MSG)-1);
        exit(1);
    }
}

static void s_catch_signals(int fd)
{
    s_fd = fd;

    struct sigaction action;
    action.sa_handler = s_signal_handler;
    action.sa_flags = 0;
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);
}

void rx_update_dataset_get_vm(randomx_cache *rx_cache, randomx_dataset **rx_dataset, randomx_vm **rx_vm, randomx_flags flags, char* rx_seedhash, const char* seedhash) 
{
    if(rx_cache == NULL || memcmp(seedhash, rx_seedhash, 32)) {
        randomx_init_cache(rx_cache, seedhash, 32);
        memcpy(rx_seedhash, seedhash, 32);
        if(flags & RANDOMX_FLAG_FULL_MEM) {
            printf("init randomx dataset\n");
            if(*rx_dataset == NULL)
                *rx_dataset = randomx_alloc_dataset(RANDOMX_FLAG_DEFAULT);
            randomx_init_dataset(*rx_dataset, rx_cache, 0, randomx_dataset_item_count());
            printf("randomx dataset init finished\n");
        }
        if(rx_vm != NULL) {
            randomx_destroy_vm(*rx_vm);
        }
        *rx_vm = randomx_create_vm(flags, rx_cache, *rx_dataset);
    }
}

static void * worker_routine(void * wid)
{
    zsock_t *worker = zsock_new_rep(wid);//">inproc://worker");
    assert(worker);

    randomx_dataset *rx_dataset;
    uint64_t rx_dataset_height;
    randomx_vm *rx_vm = NULL;

    int full_mem = 1;

    randomx_flags rx_flags = RANDOMX_FLAG_JIT | RANDOMX_FLAG_HARD_AES  | RANDOMX_FLAG_LARGE_PAGES;
    if(full_mem)
        rx_flags |= RANDOMX_FLAG_FULL_MEM;
    
    randomx_cache *rx_cache = randomx_alloc_cache(rx_flags);

    //current seed
    char *rx_seedhash = calloc(RANDOMX_HASH_SIZE, 1);
    uint64_t rx_seedheight = 0;

    char *hash = malloc(32);
    
    
    while(1) {
        zmsg_t *msg = zmsg_recv(worker);
        if(!msg)
            break;
        #ifdef DEBUG
        zmsg_print(msg);
        printf("got msg worker %s\n", wid);
        #endif

        zframe_t *msg_type = zmsg_first(msg);
        if(zframe_size(msg_type) != 1) {
            printf("invalid message type\n");
            zmsg_destroy(&msg);
            zstr_send(worker, "ERROR");
            continue;
        }

        if(*(zframe_data(msg_type)) == 'U'){
            #ifdef DEBUG
            printf("UPDATE REQ\n");
            #endif
            if(zmsg_size(msg) != 2){
                printf("U message size invalid \n");
                zmsg_destroy(&msg);
                zstr_send(worker, "ERROR");
                continue;
            }
            zframe_t *seedhash_f = zmsg_next(msg);
            if(zframe_size(seedhash_f) != 32) {
                printf("U message invalid\n");
                zstr_send(worker, "ERROR");
                zmsg_destroy(&msg);
                continue;
            }
            const char *seedhash = zframe_data(seedhash_f);

            struct timeval stop,start;
            gettimeofday(&start, NULL);

            rx_update_dataset_get_vm(rx_cache, &rx_dataset, &rx_vm, rx_flags, rx_seedhash, seedhash);
            zstr_send(worker, "UPDATED");

            gettimeofday(&stop, NULL);            
            printf("Dataset Init %s took %lu\n", wid, stop.tv_usec - start.tv_usec);
            zmsg_print(msg);

        }

        if(*(zframe_data(msg_type)) == 'H'){
            #ifdef DEBUG
            printf("HASH REQ\n");
            #endif
            if(zmsg_size(msg) != 5) {
                printf("H message size invalid \n");
                zmsg_destroy(&msg);
                zstr_send(worker, "ERROR");
                continue;
            }
            zframe_t *seedhash_f = zmsg_next(msg);
            zframe_t *mainheight_f = zmsg_next(msg);
            zframe_t *tohash_f = zmsg_next(msg);
            zframe_t *result_f = zmsg_next(msg);

            if(
                zframe_size(mainheight_f) != 8 ||
                zframe_size(seedhash_f) != 32 ||
                zframe_size(result_f) != 32) {
                printf("H message invalid\n");
                zstr_send(worker, "ERROR");
                zmsg_destroy(&msg);
                continue;
            }

            const char *seedhash = zframe_data(seedhash_f);
            uint64_t mainheight = (uint64_t) *((uint64_t*)zframe_data(mainheight_f));
            const char *tohash = zframe_data(tohash_f);
            char *result = zframe_data(result_f);

            rx_update_dataset_get_vm(rx_cache, &rx_dataset, &rx_vm, rx_flags, rx_seedhash, seedhash);

            struct timeval stop,start;
            gettimeofday(&start, NULL);

            randomx_calculate_hash(rx_vm, tohash, zframe_size(tohash_f), hash);

            gettimeofday(&stop, NULL);

            if(memcmp(hash, result, 32)) {
                #ifdef DEBUG
                printf("hash invalid\n");
                #endif
                zstr_send(worker, "INVALID");
            } else {
                #ifdef DEBUG
                printf("hash valid \n");
                #endif
                zstr_send(worker, "VALID");
            }

            printf("Hash %s height %" PRIu64 " took %lu\n", wid, mainheight,  stop.tv_usec - start.tv_usec);
        }

        zmsg_destroy(&msg);
    }
    free(hash);

}

int main(void)
{    
    int rc;
    int pipefds[2];
    rc = pipe(pipefds);
    if(rc != 0) {
        perror("creating self-pipe");
        exit(1);
    }

    for(int i=0; i<2; i++) {
        int flags = fcntl(pipefds[0], F_GETFL, 0);
        if(flags < 0) {
            perror("pipe F_GETFL");
            exit(1);
        }
        rc = fcntl(pipefds[0], F_SETFL, flags | O_NONBLOCK);
        if(rc != 0) {
            perror("pipe F_SETFL");
            exit(1);
        }
    }

    zsys_handler_set(NULL);
    s_catch_signals(pipefds[1]);

    zsock_t *frontend = zsock_new_router("@tcp://127.0.0.1:5000");
    zsock_t *worker0 = zsock_new_dealer("@inproc://worker0");
    zsock_t *worker1 = zsock_new_dealer("@inproc://worker1");

    //char *workerA_str =;

    pthread_t threads[3];
    pthread_create(&threads[0], NULL, worker_routine, ">inproc://worker0");
    pthread_create(&threads[1], NULL, worker_routine, ">inproc://worker1");

    

    void *frontend_s = zsock_resolve(frontend);
    void *worker0_s = zsock_resolve(worker0);
    void *worker1_s = zsock_resolve(worker1);

    char *seedhash_w0 = calloc(32,1);
    char *seedhash_w1 = calloc(32,1);

    int current_worker = 0;

    
    zmq_pollitem_t items [] = {
        {0, pipefds[0], ZMQ_POLLIN, 0},
        {frontend_s, 0, ZMQ_POLLIN, 0},
        {worker0_s, 0, ZMQ_POLLIN, 0},
        {worker1_s, 0, ZMQ_POLLIN, 0}
    };
    
    printf("router ready \n");

    while(1) {        
        rc = zmq_poll(items, 4, -1);
        if(rc < 0){
            if(errno == ETERM || errno == EFAULT) {
                perror("zmq_poll error");
                exit(1);
            }
        }

        #ifdef DEBUG
        printf("zmq_poll events\n");
        #endif

        if(items[0].revents & ZMQ_POLLIN) {
            char buffer[1];
            read(pipefds[0],buffer,1);
            printf("quitting\n");
            break;
        }

        if(items[1].revents & ZMQ_POLLIN) {
            zmsg_t *msg = zmsg_recv(frontend);
            if(!msg){
                break; //interrupted
            }

            //advance to start of REQ
            zframe_t *zfr = zmsg_first(msg);
            while(zfr != NULL && zframe_size(zfr) != 0) {
                zfr = zmsg_next(msg);
            }

            char req_type = 0;
            zfr = zmsg_next(msg);
            if(zfr != NULL && zframe_size(zfr) == 1) {
                req_type = *(zframe_data(zfr));
                zfr = zmsg_next(msg);
            }

            if(zfr == NULL || zframe_size(zfr) != 32){
                zmsg_print(msg);
                printf("invalid seedhash from frontend\n");
                zmsg_destroy(&msg);
                continue;                
            }

            zmsg_first(msg);

            //route to correct worker
            const char* seedhash = zframe_data(zfr);
            if(memcmp(seedhash, seedhash_w0, 32) == 0){
                #ifdef DEBUG
                printf("MATCH worker0 %c\n", req_type);
                #endif
                zmsg_send(&msg, worker0);
                if(req_type == 'H')
                    current_worker = 0;
                continue;
            }
            if(memcmp(seedhash, seedhash_w1, 32) == 0){
                #ifdef DEBUG
                printf("MATCH worker1 %c\n", req_type);
                #endif
                zmsg_send(&msg, worker1);
                if(req_type == 'H')
                    current_worker = 1;
                continue;
            }

            //choose worker to init 
            if(current_worker == 1){
                printf("INIT worker0 \n");
                zmsg_send(&msg, worker0);
                memcpy(seedhash_w0, seedhash, 32);
            } 
            else {
                printf("INIT worker1\n");
                zmsg_send(&msg, worker1);
                memcpy(seedhash_w1, seedhash, 32);
            }
                
        }
        
        if(items[2].revents & ZMQ_POLLIN) {
            zmsg_t *msg = zmsg_recv(worker0);
            if(!msg)
                break;
            zmsg_send(&msg, frontend);
        }

        if(items[3].revents & ZMQ_POLLIN) {
            zmsg_t *msg = zmsg_recv(worker1);
            if(!msg)
                break;
            zmsg_send(&msg, frontend);
        }
    }

    printf("QUITTING, closing sockets \n)");
    zsock_destroy(&frontend);
    zsock_destroy(&worker0);
    zsock_destroy(&worker1);
    return 0;

}
