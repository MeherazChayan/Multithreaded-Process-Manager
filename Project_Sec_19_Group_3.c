/*
 * Project: Multithreaded Process Manager Simulator
 * Course: CSE321 (Operating Systems Lab)
 * Group ID: 03
 * Section: 19
 * Member 1: Md. Meheraz Hossain Chayan - 22299263
 * Member 2: A.A.M Miad Islam - 22299417
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <pthread.h>
#include <time.h>
#include <ctype.h>
#include <errno.h>

#define MAX_PROCESSES 64
#define MAX_CHILDREN 64
#define LINE_BUF 256
#define ACTION_BUF 128
#define SNAPSHOT_BUF 8192
#define EVENT_CAP 512

#if defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 201112L)
static _Thread_local int current_worker_id = -1;
#else
static __thread int current_worker_id = -1;
#endif

typedef enum { RUNNING, BLOCKED, ZOMBIE } ProcessState;

typedef struct {
    int used, pid, ppid, exit_status;
    ProcessState state;
    int children[MAX_CHILDREN], child_count, waiters;
    unsigned long long exit_order;
    pthread_cond_t wait_cond;
} PCB;

typedef struct {
    char title[ACTION_BUF];
    char snapshot[SNAPSHOT_BUF];
} Event;

typedef struct {
    int thread_id;
    const char *filename;
} WorkerArgs;

static PCB table[MAX_PROCESSES];
static int next_pid = 2;
static unsigned long long next_exit_order = 1;

static pthread_mutex_t table_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t q_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t q_not_empty = PTHREAD_COND_INITIALIZER;
static pthread_cond_t q_not_full = PTHREAD_COND_INITIALIZER;

static Event event_q[EVENT_CAP];
static int q_head = 0, q_tail = 0, q_count = 0, monitor_stop = 0;

static void die(const char *msg) { perror(msg); exit(EXIT_FAILURE); }

static const char *state_name(ProcessState s) {
    return s == RUNNING ? "RUNNING" : s == BLOCKED ? "BLOCKED" : "ZOMBIE";
}

static int find_index_locked(int pid) {
    for (int i = 0; i < MAX_PROCESSES; i++)
        if (table[i].used && table[i].pid == pid) return i;
    return -1;
}

static int find_free_locked(void) {
    for (int i = 0; i < MAX_PROCESSES; i++)
        if (!table[i].used) return i;
    return -1;
}

static int has_child_locked(int parent_pid, int child_pid) {
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (!table[i].used || table[i].ppid != parent_pid) continue;
        if (child_pid == -1 || table[i].pid == child_pid) return 1;
    }
    return 0;
}

static int zombie_child_locked(int parent_pid, int child_pid) {
    int best = -1;
    unsigned long long best_order = 0;
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (!table[i].used || table[i].ppid != parent_pid || table[i].state != ZOMBIE) continue;
        if (child_pid != -1) {
            if (table[i].pid == child_pid) return i;
            continue;
        }
        if (best == -1 || table[i].exit_order < best_order) {
            best = i;
            best_order = table[i].exit_order;
        }
    }
    return best;
}

static void remove_child_locked(int parent_pid, int child_pid) {
    int p = find_index_locked(parent_pid);
    if (p == -1) return;
    PCB *parent = &table[p];
    for (int i = 0; i < parent->child_count; i++) {
        if (parent->children[i] != child_pid) continue;
        for (int j = i; j < parent->child_count - 1; j++)
            parent->children[j] = parent->children[j + 1];
        parent->child_count--;
        return;
    }
}

static void refresh_wait_state_locked(PCB *p) {
    if (p && p->state != ZOMBIE) p->state = p->waiters > 0 ? BLOCKED : RUNNING;
}

static void delete_process_locked(int i) {
    if (i < 0 || i >= MAX_PROCESSES || !table[i].used) return;
    pthread_cond_destroy(&table[i].wait_cond);
    memset(&table[i], 0, sizeof(table[i]));
}

static void build_snapshot_locked(char *out, size_t n) {
    size_t off = 0;
    int used[MAX_PROCESSES] = {0};
    off += (size_t)snprintf(out + off, n - off,
            "PID    PPID   STATE      EXIT_STATUS\n"
            "---------------------------------------------\n");
    for (int c = 0; c < MAX_PROCESSES; c++) {
        int best = -1;
        for (int i = 0; i < MAX_PROCESSES; i++) {
            if (!table[i].used || used[i]) continue;
            if (best == -1 || table[i].pid < table[best].pid) best = i;
        }
        if (best == -1) break;
        used[best] = 1;
        off += (size_t)snprintf(out + off, n - off, "%-6d %-6d %-10s ",
                                table[best].pid, table[best].ppid, state_name(table[best].state));
        if (table[best].state == ZOMBIE) {
            off += (size_t)snprintf(out + off, n - off, "%d\n", table[best].exit_status);
        } else {
            off += (size_t)snprintf(out + off, n - off, "-\n");
        }
        if (off >= n) { out[n - 1] = '\0'; break; }
    }
}

static void queue_event(const char *title, const char *snapshot) {
    if (pthread_mutex_lock(&q_mutex) != 0) die("pthread_mutex_lock");
    while (q_count == EVENT_CAP) {
        if (pthread_cond_wait(&q_not_full, &q_mutex) != 0) die("pthread_cond_wait");
    }
    snprintf(event_q[q_tail].title, ACTION_BUF, "%s", title);
    snprintf(event_q[q_tail].snapshot, SNAPSHOT_BUF, "%s", snapshot);
    q_tail = (q_tail + 1) % EVENT_CAP;
    q_count++;
    if (pthread_cond_signal(&q_not_empty) != 0) die("pthread_cond_signal");
    if (pthread_mutex_unlock(&q_mutex) != 0) die("pthread_mutex_unlock");
}

static void log_change_locked(const char *fmt, ...) {
    char title[ACTION_BUF], snap[SNAPSHOT_BUF];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(title, sizeof(title), fmt, ap);
    va_end(ap);
    build_snapshot_locked(snap, sizeof(snap));
    queue_event(title, snap);
}

static int terminate_locked(int pid, int status, const char *title) {
    int i = find_index_locked(pid);
    if (i == -1 || table[i].state == ZOMBIE) return -1;
    table[i].state = ZOMBIE;
    table[i].exit_status = status;
    table[i].exit_order = next_exit_order++;
    int p = find_index_locked(table[i].ppid);
    if (p != -1) pthread_cond_broadcast(&table[p].wait_cond);
    pthread_cond_broadcast(&table[i].wait_cond);
    log_change_locked("%s", title);
    return 0;
}

static void init_manager(void) {
    memset(table, 0, sizeof(table));
    table[0].used = 1;
    table[0].pid = 1;
    table[0].ppid = 0;
    table[0].state = RUNNING;
    if (pthread_cond_init(&table[0].wait_cond, NULL) != 0) die("pthread_cond_init");
}

int pm_fork(int parent_pid) {
    char title[ACTION_BUF];
    snprintf(title, sizeof(title), "Thread %d calls pm_fork %d", current_worker_id, parent_pid);
    if (pthread_mutex_lock(&table_mutex) != 0) die("pthread_mutex_lock");
    int p = find_index_locked(parent_pid), f = find_free_locked();
    if (p == -1 || table[p].state == ZOMBIE || f == -1 || table[p].child_count >= MAX_CHILDREN) {
        pthread_mutex_unlock(&table_mutex);
        return -1;
    }
    memset(&table[f], 0, sizeof(table[f]));
    table[f].used = 1;
    table[f].pid = next_pid++;
    table[f].ppid = parent_pid;
    table[f].state = RUNNING;
    if (pthread_cond_init(&table[f].wait_cond, NULL) != 0) die("pthread_cond_init");
    table[p].children[table[p].child_count++] = table[f].pid;
    int child_pid = table[f].pid;
    log_change_locked("%s", title);
    if (pthread_mutex_unlock(&table_mutex) != 0) die("pthread_mutex_unlock");
    return child_pid;
}

int pm_exit(int pid, int status) {
    char title[ACTION_BUF];
    snprintf(title, sizeof(title), "Thread %d calls pm_exit %d %d", current_worker_id, pid, status);
    if (pthread_mutex_lock(&table_mutex) != 0) die("pthread_mutex_lock");
    int r = terminate_locked(pid, status, title);
    if (pthread_mutex_unlock(&table_mutex) != 0) die("pthread_mutex_unlock");
    return r;
}

int pm_kill(int pid) {
    char title[ACTION_BUF];
    snprintf(title, sizeof(title), "Thread %d calls pm_kill %d", current_worker_id, pid);
    if (pthread_mutex_lock(&table_mutex) != 0) die("pthread_mutex_lock");
    int r = terminate_locked(pid, -9, title);
    if (pthread_mutex_unlock(&table_mutex) != 0) die("pthread_mutex_unlock");
    return r;
}

int pm_wait(int parent_pid, int child_pid) {
    int registered = 0;
    char block[ACTION_BUF], done[ACTION_BUF], trivial[ACTION_BUF];
    snprintf(block, sizeof(block), "Thread %d blocks in pm_wait %d %d", current_worker_id, parent_pid, child_pid);
    snprintf(done, sizeof(done), "Thread %d completes pm_wait %d %d", current_worker_id, parent_pid, child_pid);
    snprintf(trivial, sizeof(trivial), "Thread %d returns trivially from pm_wait %d %d", current_worker_id, parent_pid, child_pid);

    if (pthread_mutex_lock(&table_mutex) != 0) die("pthread_mutex_lock");
    while (1) {
        int p = find_index_locked(parent_pid);
        if (p == -1) { pthread_mutex_unlock(&table_mutex); return -1; }
        PCB *parent = &table[p];

        if (parent->state == ZOMBIE) {
            if (registered && parent->waiters > 0) parent->waiters--;
            pthread_mutex_unlock(&table_mutex);
            return -1;
        }
        if (!has_child_locked(parent_pid, child_pid)) {
            if (registered && parent->waiters > 0) {
                parent->waiters--;
                refresh_wait_state_locked(parent);
                log_change_locked("%s", trivial);
            }
            if (pthread_mutex_unlock(&table_mutex) != 0) die("pthread_mutex_unlock");
            return 0;
        }
        int z = zombie_child_locked(parent_pid, child_pid);
        if (z != -1) {
            int status = table[z].exit_status, child_real_pid = table[z].pid;
            if (registered && parent->waiters > 0) parent->waiters--;
            refresh_wait_state_locked(parent);
            remove_child_locked(parent_pid, child_real_pid);
            delete_process_locked(z);
            log_change_locked("%s", done);
            if (pthread_mutex_unlock(&table_mutex) != 0) die("pthread_mutex_unlock");
            return status;
        }
        if (!registered) {
            parent->waiters++;
            registered = 1;
            refresh_wait_state_locked(parent);
            log_change_locked("%s", block);
        }
        if (pthread_cond_wait(&parent->wait_cond, &table_mutex) != 0) die("pthread_cond_wait");
    }
}

void pm_ps(void) {
    char snap[SNAPSHOT_BUF];
    if (pthread_mutex_lock(&table_mutex) != 0) die("pthread_mutex_lock");
    build_snapshot_locked(snap, sizeof(snap));
    if (pthread_mutex_unlock(&table_mutex) != 0) die("pthread_mutex_unlock");
    printf("%s\n", snap);
    fflush(stdout);
}

static void trim(char *s) {
    char *p = s;
    while (*p && isspace((unsigned char)*p)) p++;
    if (p != s) memmove(s, p, strlen(p) + 1);
    for (int n = (int)strlen(s); n > 0 && isspace((unsigned char)s[n - 1]); n--) s[n - 1] = '\0';
}

static void sleep_ms(int ms) {
    struct timespec t = { ms / 1000, (long)(ms % 1000) * 1000000L };
    while (nanosleep(&t, &t) == -1 && errno == EINTR) {}
}

static void run_command(const char *file, int line_no, char *line) {
    char cmd[16];
    trim(line);
    if (!line[0] || line[0] == '#') return;
    if (sscanf(line, "%15s", cmd) != 1) return;

    if (!strcmp(cmd, "fork")) {
        int p; if (sscanf(line, "fork %d", &p) == 1) {
            if (pm_fork(p) == -1) fprintf(stderr, "[%s:%d] fork failed: %s\n", file, line_no, line);
        } else fprintf(stderr, "[%s:%d] invalid fork command\n", file, line_no);
    } else if (!strcmp(cmd, "exit")) {
        int p, s; if (sscanf(line, "exit %d %d", &p, &s) == 2) {
            if (pm_exit(p, s) == -1) fprintf(stderr, "[%s:%d] exit failed: %s\n", file, line_no, line);
        } else fprintf(stderr, "[%s:%d] invalid exit command\n", file, line_no);
    } else if (!strcmp(cmd, "wait")) {
        int p, c; if (sscanf(line, "wait %d %d", &p, &c) == 2) (void)pm_wait(p, c);
        else fprintf(stderr, "[%s:%d] invalid wait command\n", file, line_no);
    } else if (!strcmp(cmd, "kill")) {
        int p; if (sscanf(line, "kill %d", &p) == 1) {
            if (pm_kill(p) == -1) fprintf(stderr, "[%s:%d] kill failed: %s\n", file, line_no, line);
        } else fprintf(stderr, "[%s:%d] invalid kill command\n", file, line_no);
    } else if (!strcmp(cmd, "sleep")) {
        int ms; if (sscanf(line, "sleep %d", &ms) == 1 && ms >= 0) sleep_ms(ms);
        else fprintf(stderr, "[%s:%d] invalid sleep command\n", file, line_no);
    } else if (!strcmp(cmd, "ps")) {
        pm_ps();
    } else {
        fprintf(stderr, "[%s:%d] unknown command: %s\n", file, line_no, line);
    }
}

static void *worker_thread(void *arg) {
    WorkerArgs *w = (WorkerArgs *)arg;
    FILE *fp = fopen(w->filename, "r");
    if (!fp) {
        fprintf(stderr, "Could not open %s: %s\n", w->filename, strerror(errno));
        return NULL;
    }
    current_worker_id = w->thread_id;
    char line[LINE_BUF];
    int line_no = 0;
    while (fgets(line, sizeof(line), fp)) run_command(w->filename, ++line_no, line);
    fclose(fp);
    return NULL;
}

static void *monitor_thread(void *arg) {
    (void)arg;
    FILE *fp = fopen("snapshots.txt", "w");
    if (!fp) {
        fprintf(stderr, "Could not open snapshots.txt: %s\n", strerror(errno));
        return NULL;
    }
    while (1) {
        Event e;
        if (pthread_mutex_lock(&q_mutex) != 0) die("pthread_mutex_lock");
        while (q_count == 0 && !monitor_stop)
            if (pthread_cond_wait(&q_not_empty, &q_mutex) != 0) die("pthread_cond_wait");
        if (q_count == 0 && monitor_stop) { pthread_mutex_unlock(&q_mutex); break; }
        e = event_q[q_head];
        q_head = (q_head + 1) % EVENT_CAP;
        q_count--;
        if (pthread_cond_signal(&q_not_full) != 0) die("pthread_cond_signal");
        if (pthread_mutex_unlock(&q_mutex) != 0) die("pthread_mutex_unlock");
        fprintf(fp, "%s\n%s\n", e.title, e.snapshot);
        fflush(fp);
    }
    fclose(fp);
    return NULL;
}

static void queue_initial_snapshot(void) {
    char snap[SNAPSHOT_BUF];
    if (pthread_mutex_lock(&table_mutex) != 0) die("pthread_mutex_lock");
    build_snapshot_locked(snap, sizeof(snap));
    if (pthread_mutex_unlock(&table_mutex) != 0) die("pthread_mutex_unlock");
    queue_event("Initial Process Table", snap);
}

static void cleanup(void) {
    for (int i = 0; i < MAX_PROCESSES; i++)
        if (table[i].used) pthread_cond_destroy(&table[i].wait_cond);
    pthread_mutex_destroy(&table_mutex);
    pthread_mutex_destroy(&q_mutex);
    pthread_cond_destroy(&q_not_empty);
    pthread_cond_destroy(&q_not_full);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s thread0.txt [thread1.txt ...]\n", argv[0]);
        return EXIT_FAILURE;
    }

    init_manager();

    pthread_t monitor;
    if (pthread_create(&monitor, NULL, monitor_thread, NULL) != 0) die("pthread_create");
    queue_initial_snapshot();

    int n = argc - 1;
    pthread_t *workers = malloc((size_t)n * sizeof(*workers));
    WorkerArgs *args = malloc((size_t)n * sizeof(*args));
    if (!workers || !args) die("malloc");

    for (int i = 0; i < n; i++) {
        args[i].thread_id = i;
        args[i].filename = argv[i + 1];
        if (pthread_create(&workers[i], NULL, worker_thread, &args[i]) != 0) die("pthread_create");
    }
    for (int i = 0; i < n; i++)
        if (pthread_join(workers[i], NULL) != 0) die("pthread_join");

    free(workers);
    free(args);

    if (pthread_mutex_lock(&q_mutex) != 0) die("pthread_mutex_lock");
    monitor_stop = 1;
    if (pthread_cond_signal(&q_not_empty) != 0) die("pthread_cond_signal");
    if (pthread_mutex_unlock(&q_mutex) != 0) die("pthread_mutex_unlock");

    if (pthread_join(monitor, NULL) != 0) die("pthread_join");
    cleanup();
    return EXIT_SUCCESS;
}
