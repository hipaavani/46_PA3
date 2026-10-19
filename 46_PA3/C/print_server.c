#include "job_queue.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>

#define NUM_PRINTERS 4
#define DEFAULT_JOBS 50

typedef struct {
    int id;
    job_queue_t *queue;
    long jobs_completed;
} worker_arg_t;

typedef struct {
    int id;
    job_queue_t *queue;
    long jobs_to_submit;
    long jobs_submitted;
} producer_arg_t;

static void simulate_printing(void)
{
    struct timespec ts = {.tv_sec = 0, .tv_nsec = 1000000L}; /* 1 ms */
    (void)nanosleep(&ts, NULL);
}

static void *printer_worker(void *arg)
{
    worker_arg_t *w = (worker_arg_t *)arg;
    int job = 0;

    while (queue_pop(w->queue, &job)) {
        simulate_printing();
        w->jobs_completed++;
        printf("PROCESSED job=%d printer=%d\n", job, w->id);
        fflush(stdout);
    }

    return NULL;
}

static void *producer_worker(void *arg)
{
    producer_arg_t *p = (producer_arg_t *)arg;

    for (long j = 1; j <= p->jobs_to_submit; j++) {
        long job_id = (long)p->id * p->jobs_to_submit + j;

        if (queue_push(p->queue, (int)job_id) != 0) {
            fprintf(stderr, "Producer %d failed to submit job %ld\n",
                    p->id, job_id);
            break;
        }

        printf("SUBMITTED producer=%d job=%ld\n", p->id, job_id);
        fflush(stdout);
        p->jobs_submitted++;
    }

    return NULL;
}

static long parse_positive_long(const char *s, const char *name)
{
    char *end = NULL;
    errno = 0;
    long value = strtol(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' || value <= 0) {
        fprintf(stderr, "Invalid %s: %s\n", name, s);
        exit(EXIT_FAILURE);
    }
    return value;
}

static int run_core_server(long num_jobs)
{
    job_queue_t queue;
    pthread_t printers[NUM_PRINTERS];
    worker_arg_t workers[NUM_PRINTERS] = {0};

    if (queue_init(&queue) != 0) {
        fprintf(stderr, "queue_init failed\n");
        return EXIT_FAILURE;
    }

    

    /* TODO C13:
     * Initialize workers[i] and create NUM_PRINTERS printer_worker threads.
     */
        int printers_created = 0;

    for (int i = 0; i < NUM_PRINTERS; i++) {
        workers[i].id = i + 1;
        workers[i].queue = &queue;
        workers[i].jobs_completed = 0;

        if (pthread_create(&printers[i], NULL, printer_worker,
                           &workers[i]) != 0) {
            fprintf(stderr, "Failed to create printer thread\n");

            queue_shutdown(&queue);

            for (int j = 0; j < printers_created; j++) {
                pthread_join(printers[j], NULL);
            }

            queue_destroy(&queue);
            return EXIT_FAILURE;
        }

        printers_created++;
    }

    

    for (long job = 1; job <= num_jobs; job++) {
        if (queue_push(&queue, (int)job) != 0) {
            fprintf(stderr, "Failed to submit job %ld\n", job);
            queue_shutdown(&queue);
            queue_destroy(&queue);
            return EXIT_FAILURE;
        }
        printf("SUBMITTED job=%ld\n", job);
    }

    /* TODO C14: request graceful shutdown after the last submission. */
    queue_shutdown(&queue);

    /* TODO C15: join all printer threads. */

    for (int i = 0; i < NUM_PRINTERS; i++) {
    if (pthread_join(printers[i], NULL) != 0) {
        fprintf(stderr, "Failed to join printer thread\n");
    }
}

    long total = 0;
    for (int i = 0; i < NUM_PRINTERS; i++) {
        total += workers[i].jobs_completed;
    }

    printf("TOTAL_COMPLETED %ld\n", total);

    queue_destroy(&queue);
    return (total == num_jobs) ? EXIT_SUCCESS : EXIT_FAILURE;
}

/*
 * BONUS (+10): Multiple concurrent client/producers.
 *
 * Required command line when implemented:
 *     ./print_server --bonus <producers> <jobs-per-producer>
 *
 * Each producer must run as its own pthread and submit exactly the requested
 * number of jobs into the SAME bounded queue while the printer threads consume
 * concurrently. Use unique positive job IDs. For every accepted job print:
 *
 *     SUBMITTED producer=<p> job=<id>
 *
 * The printer output remains:
 *
 *     PROCESSED job=<id> printer=<id>
 *
 * Main must join all producer threads BEFORE calling queue_shutdown(). Then it
 * must join the printer threads and print:
 *
 *     TOTAL_COMPLETED <n>
 *
 * The autograder awards bonus credit only when every submitted job is processed
 * exactly once and all producer counts are correct.
 */
static int run_bonus_server(long producers, long jobs_per_producer)
{
    if (producers > INT_MAX || jobs_per_producer > INT_MAX ||
        producers > INT_MAX / jobs_per_producer) {
        fprintf(stderr, "Too many jobs requested\n");
        return EXIT_FAILURE;
    }

    long total_jobs = producers * jobs_per_producer;

    job_queue_t queue;
    pthread_t printers[NUM_PRINTERS];
    worker_arg_t workers[NUM_PRINTERS] = {0};

    pthread_t *producer_threads = calloc((size_t)producers,
                                         sizeof(*producer_threads));
    producer_arg_t *producer_args = calloc((size_t)producers,
                                           sizeof(*producer_args));

    if (producer_threads == NULL || producer_args == NULL) {
        fprintf(stderr, "Failed to allocate producer resources\n");
        free(producer_threads);
        free(producer_args);
        return EXIT_FAILURE;
    }

    if (queue_init(&queue) != 0) {
        fprintf(stderr, "queue_init failed\n");
        free(producer_threads);
        free(producer_args);
        return EXIT_FAILURE;
    }

    int printers_created = 0;
    int producers_created = 0;
    int failed = 0;

    for (int i = 0; i < NUM_PRINTERS; i++) {
        workers[i].id = i + 1;
        workers[i].queue = &queue;
        workers[i].jobs_completed = 0;

        if (pthread_create(&printers[i], NULL, printer_worker,
                           &workers[i]) != 0) {
            fprintf(stderr, "Failed to create printer thread\n");
            failed = 1;
            break;
        }

        printers_created++;
    }

    if (!failed) {
        for (long i = 0; i < producers; i++) {
            producer_args[i].id = (int)i;
            producer_args[i].queue = &queue;
            producer_args[i].jobs_to_submit = jobs_per_producer;
            producer_args[i].jobs_submitted = 0;

            if (pthread_create(&producer_threads[i], NULL, producer_worker,
                               &producer_args[i]) != 0) {
                fprintf(stderr, "Failed to create producer thread\n");
                failed = 1;
                break;
            }

            producers_created++;
        }
    }

    for (int i = 0; i < producers_created; i++) {
        if (pthread_join(producer_threads[i], NULL) != 0) {
            fprintf(stderr, "Failed to join producer thread\n");
            failed = 1;
        }
    }

    queue_shutdown(&queue);

    for (int i = 0; i < printers_created; i++) {
        if (pthread_join(printers[i], NULL) != 0) {
            fprintf(stderr, "Failed to join printer thread\n");
            failed = 1;
        }
    }

    long completed = 0;
    for (int i = 0; i < printers_created; i++) {
        completed += workers[i].jobs_completed;
    }

    printf("TOTAL_COMPLETED %ld\n", completed);

    for (int i = 0; i < producers_created; i++) {
        if (producer_args[i].jobs_submitted != jobs_per_producer) {
            failed = 1;
        }
    }

    if (producers_created != producers || printers_created != NUM_PRINTERS ||
        completed != total_jobs) {
        failed = 1;
    }

    queue_destroy(&queue);
    free(producer_threads);
    free(producer_args);

    return failed ? EXIT_FAILURE : EXIT_SUCCESS;
}

int main(int argc, char *argv[])
{
    if (argc >= 2 && strcmp(argv[1], "--bonus") == 0) {
        if (argc != 4) {
            fprintf(stderr, "Usage: %s --bonus <producers> <jobs-per-producer>\n", argv[0]);
            return EXIT_FAILURE;
        }
        long producers = parse_positive_long(argv[2], "producer count");
        long jobs = parse_positive_long(argv[3], "jobs per producer");
        return run_bonus_server(producers, jobs);
    }

    if (argc > 2) {
        fprintf(stderr, "Usage: %s [number-of-jobs]\n", argv[0]);
        return EXIT_FAILURE;
    }

    long num_jobs = (argc == 2) ? parse_positive_long(argv[1], "job count") : DEFAULT_JOBS;
    return run_core_server(num_jobs);
}
