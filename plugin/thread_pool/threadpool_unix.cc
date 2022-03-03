/* Copyright (C) 2012 Monty Program Ab

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */
#include <atomic>

#include <time.h>
#include "my_sys.h"
#include "my_systime.h"
#include "mysql/thread_pool_priv.h"  // thd_is_transaction_active()
#include "sql/debug_sync.h"
#include "sql/log.h"
#include "sql/protocol_classic.h"
#include "threadpool.h"
#include "threadpool_unix.h"

#define MYSQL_SERVER 1

/** Maximum number of native events a listener can read in one go */
#define MAX_EVENTS 1024

/** Define if wait_begin() should create threads if necessary without waiting
for stall detection to kick in */
#define THREADPOOL_CREATE_THREADS_ON_WAIT

/** Indicates that threadpool was initialized*/
static bool threadpool_started = false;

/*
  Define PSI Keys for performance schema.
  We have a mutex per group, worker threads, condition per worker thread,
  and timer thread  with its own mutex and condition.
*/

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key key_group_mutex;
static PSI_mutex_key key_timer_mutex;
static PSI_mutex_info mutex_list[] = {
    {&key_group_mutex, "group_mutex", 0, 0, PSI_DOCUMENT_ME},
    {&key_timer_mutex, "timer_mutex", PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME}};

static PSI_cond_key key_worker_cond;
static PSI_cond_key key_timer_cond;
static PSI_cond_info cond_list[] = {
    {&key_worker_cond, "worker_cond", 0, 0, PSI_DOCUMENT_ME},
    {&key_timer_cond, "timer_cond", PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME}};

static PSI_thread_key key_worker_thread;
static PSI_thread_key key_timer_thread;
static PSI_thread_info thread_list[] = {
    {&key_worker_thread, "worker_thread", 0, 0, PSI_DOCUMENT_ME},
    {&key_timer_thread, "timer_thread", PSI_FLAG_SINGLETON, 0,
     PSI_DOCUMENT_ME}};
#endif  // HAVE_PSI_INTERFACE

thread_group_t all_groups[MAX_THREAD_GROUPS];
static uint group_count;

/**
 Used for printing "pool blocked" message, see
 print_pool_blocked_message();
*/
static ulonglong pool_block_start;

/* Global timer for all groups  */
struct pool_timer_t {
  mysql_mutex_t mutex;
  mysql_cond_t cond;
  std::atomic<uint64> current_microtime;
  std::atomic<uint64> next_timeout_check;
  int tick_interval;
  bool shutdown;
};

static pool_timer_t pool_timer;

static void queue_put(thread_group_t *thread_group, connection_t *connection);
static int wake_thread(thread_group_t *thread_group,
                       bool due_to_stall) noexcept;
static void handle_event(connection_t *connection);
static int wake_or_create_thread(thread_group_t *thread_group,
                                 bool due_to_stall = false,
                                 bool admin_connection = false);
static int create_worker(thread_group_t *thread_group, bool due_to_stall,
                         bool admin_connection = false) noexcept;
static void *worker_main(void *param);
static void check_stall(thread_group_t *thread_group);
static void connection_abort(connection_t *connection);
static void set_next_timeout_check(ulonglong abstime);
static void print_pool_blocked_message(bool) noexcept;


int vio_cancel(Vio *vio, int how)
{
  int r= 0;
  DBUG_ENTER("vio_cancel");

  if (vio->inactive == FALSE)
  {
    assert(vio->type ==  VIO_TYPE_TCPIP ||
      vio->type == VIO_TYPE_SOCKET ||
      vio->type == VIO_TYPE_SSL);

    assert(mysql_socket_getfd(vio->mysql_socket) >= 0);
    if (mysql_socket_shutdown(vio->mysql_socket, how))
      r= -1;
  }

  DBUG_RETURN(r);
}

/**
 Asynchronous network IO.

 We use native edge-triggered network IO multiplexing facility.
 This maps to different APIs on different Unixes.

 Supported are currently Linux with epoll, Solaris with event ports,
 OSX and BSD with kevent. All those API's are used with one-shot flags
 (the event is signalled once client has written something into the socket,
 then socket is removed from the "poll-set" until the  command is finished,
 and we need to re-arm/re-register socket)

 No implementation for poll/select/AIO is currently provided.

 The API closely resembles all of the above mentioned platform APIs
 and consists of following functions.

 - io_poll_create()
 Creates an io_poll descriptor
 On Linux: epoll_create()

 - io_poll_associate_fd(int poll_fd, int fd, void *data)
 Associate file descriptor with io poll descriptor
 On Linux : epoll_ctl(..EPOLL_CTL_ADD))

 - io_poll_disassociate_fd(int pollfd, int fd)
  Associate file descriptor with io poll descriptor
  On Linux: epoll_ctl(..EPOLL_CTL_DEL)


 - io_poll_start_read(int poll_fd,int fd, void *data)
 The same as io_poll_associate_fd(), but cannot be used before
 io_poll_associate_fd() was called.
 On Linux : epoll_ctl(..EPOLL_CTL_MOD)

 - io_poll_wait (int pollfd, native_event *native_events, int maxevents,
   int timeout_ms)

 wait until one or more descriptors added with io_poll_associate_fd()
 or io_poll_start_read() becomes readable. Data associated with
 descriptors can be retrieved from native_events array, using
 native_event_get_userdata() function.


 On Linux: epoll_wait()
*/

#if defined(__linux__)
#ifndef EPOLLRDHUP
/* Early 2.6 kernel did not have EPOLLRDHUP */
#define EPOLLRDHUP 0
#endif
static int io_poll_create() noexcept { return epoll_create(1); }

static int io_poll_associate_fd(int pollfd, int fd, void *data) noexcept {
  struct epoll_event ev;
  ev.data.u64 = 0; /* Keep valgrind happy */
  ev.data.ptr = data;
  ev.events = EPOLLIN | EPOLLET | EPOLLERR | EPOLLRDHUP | EPOLLONESHOT;
  return epoll_ctl(pollfd, EPOLL_CTL_ADD, fd, &ev);
}

static int io_poll_start_read(int pollfd, int fd, void *data) noexcept {
  struct epoll_event ev;
  ev.data.u64 = 0; /* Keep valgrind happy */
  ev.data.ptr = data;
  ev.events = EPOLLIN | EPOLLET | EPOLLERR | EPOLLRDHUP | EPOLLONESHOT;
  return epoll_ctl(pollfd, EPOLL_CTL_MOD, fd, &ev);
}

static int io_poll_disassociate_fd(int pollfd, int fd) noexcept {
  struct epoll_event ev;
  return epoll_ctl(pollfd, EPOLL_CTL_DEL, fd, &ev);
}

/*
 Wrapper around epoll_wait.
 NOTE - in case of EINTR, it restarts with original timeout. Since we use
 either infinite or 0 timeouts, this is not critical
*/
static int io_poll_wait(int pollfd, native_event *native_events, int maxevents,
                        int timeout_ms) noexcept {
  int ret;
  do {
    ret = epoll_wait(pollfd, native_events, maxevents, timeout_ms);
  } while (ret == -1 && errno == EINTR);
  return ret;
}

static void *native_event_get_userdata(native_event *event) noexcept {
  return event->data.ptr;
}

#elif defined(__FreeBSD__) || defined(__APPLE__)
static int io_poll_create() noexcept { return kqueue(); }

static int io_poll_start_read(int pollfd, int fd, void *data) noexcept {
  struct kevent ke;
  EV_SET(&ke, fd, EVFILT_READ, EV_ADD | EV_ONESHOT, 0, 0, data);
  return kevent(pollfd, &ke, 1, 0, 0, 0);
}

static int io_poll_associate_fd(int pollfd, int fd, void *data) noexcept {
  struct kevent ke;
  EV_SET(&ke, fd, EVFILT_READ, EV_ADD | EV_ONESHOT, 0, 0, data);
  return io_poll_start_read(pollfd, fd, data);
}

static int io_poll_disassociate_fd(int pollfd, int fd) noexcept {
  struct kevent ke;
  EV_SET(&ke, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
  return kevent(pollfd, &ke, 1, 0, 0, 0);
}

static int io_poll_wait(int pollfd, struct kevent *events, int maxevents,
                        int timeout_ms) noexcept {
  struct timespec ts;
  int ret;
  if (timeout_ms >= 0) {
    ts.tv_sec = timeout_ms / 1000;
    ts.tv_nsec = (timeout_ms % 1000) * 1000000;
  }
  do {
    ret = kevent(pollfd, 0, 0, events, maxevents,
                 (timeout_ms >= 0) ? &ts : nullptr);
  } while (ret == -1 && errno == EINTR);
  return ret;
}

static void *native_event_get_userdata(native_event *event) noexcept {
  return event->udata;
}
#else
#error not ported yet to this OS
#endif

namespace {

/*
  Prevent too many active threads executing at the same time, if the workload is
  not CPU bound.
*/

inline bool too_many_active_threads(
    const thread_group_t &thread_group) noexcept {
  return (thread_group.active_thread_count >=
              1 + (int)threadpool_oversubscribe &&
          !thread_group.stalled);
}

/*
  Limit the number of 'busy' threads by 1 + thread_pool_oversubscribe. A thread
  is busy if it is in either the active state or the waiting state (i.e. between
  thd_wait_begin() / thd_wait_end() calls).
*/

inline bool too_many_busy_threads(const thread_group_t &thread_group) noexcept {
  return (thread_group.active_thread_count + thread_group.waiting_thread_count >
          1 + (int)threadpool_oversubscribe);
}

/*
   Checks if a given connection is eligible to enter the high priority queue
   based on its current thread_pool_high_prio_mode value, available high
   priority tickets and transactional state and whether any locks are held.
*/

inline bool connection_is_high_prio(const connection_t &c) noexcept {
  // const ulong mode = c.thd->variables.threadpool_high_prio_mode;
  const ulong mode = c.threadpool_high_prio_mode;  // !! todo

  return (mode == TP_HIGH_PRIO_MODE_STATEMENTS) ||
         (mode == TP_HIGH_PRIO_MODE_TRANSACTIONS && c.tickets > 0 &&
          (thd_is_transaction_active(c.thd) ||
           c.thd->variables.option_bits & OPTION_TABLE_LOCK ||
           c.thd->locked_tables_mode != LTM_NONE ||
           c.thd->mdl_context.has_locks() ||
           c.thd->global_read_lock.is_acquired() ||
           c.thd->mdl_context.has_locks(MDL_key::USER_LEVEL_LOCK) ||
           c.thd->mdl_context.has_locks(MDL_key::LOCKING_SERVICE)));
}

}  // namespace


/* Dequeue element from a workqueue */

static connection_t *queue_get(thread_group_t *thread_group) noexcept {
  DBUG_ENTER("queue_get");
  thread_group->queue_event_count++;
  connection_t *c;

  if ((c = thread_group->high_prio_queue.front())) {
    thread_group->high_prio_queue.remove(c);
  }
  /*
    Don't pick events from the low priority queue if there are too many
    active + waiting threads.
  */
  else if (!too_many_busy_threads(*thread_group) &&
           (c = thread_group->queue.front())) {
    thread_group->queue.remove(c);
  }
  DBUG_RETURN(c);
}

static connection_t *queue_get(thread_group_t *group, operation_origin origin) {
  connection_t *ret = queue_get(group);
  if (ret != nullptr) {
    TP_INCREMENT_GROUP_COUNTER(group, dequeues[(int)origin]);
  }
  return ret;
}

static inline void queue_push(thread_group_t *thread_group, connection_t *connection)
{
  connection->enqueue_time= pool_timer.current_microtime;
  thread_group->queue.push_back(connection);
}

static inline void high_prio_queue_push(thread_group_t *thread_group, connection_t *connection)
{
  connection->enqueue_time= pool_timer.current_microtime;
  thread_group->high_prio_queue.push_back(connection);
}

class Thd_timeout_checker : public Do_THD_Impl {
 private:
  pool_timer_t *const m_timer;

 public:
  Thd_timeout_checker(pool_timer_t *timer) noexcept : m_timer(timer) {}

  virtual ~Thd_timeout_checker() {}

  virtual void operator()(THD *thd) noexcept {
    if (thd_get_net_read_write(thd) != 1) return;

    connection_t *connection = (connection_t *)thd->scheduler.data;
    if (!connection) return;

    if (connection->abs_wait_timeout <
        m_timer->current_microtime.load(std::memory_order_relaxed)) {
      /* Wait timeout exceeded, kill connection. */
      mysql_mutex_lock(&thd->LOCK_thd_data);
      thd->killed = THD::KILL_CONNECTION;
      tp_post_kill_notification(thd);
      mysql_mutex_unlock(&thd->LOCK_thd_data);
    } else {
      set_next_timeout_check(connection->abs_wait_timeout);
    }
  }
};

/*
  Handle wait timeout :
  Find connections that have been idle for too long and kill them.
  Also, recalculate time when next timeout check should run.
*/

static void timeout_check(pool_timer_t *timer) {
  DBUG_ENTER("timeout_check");

  /* Reset next timeout check, it will be recalculated in the loop below */
  timer->next_timeout_check.store(ULLONG_MAX, std::memory_order_relaxed);

  Thd_timeout_checker thd_timeout_checker(timer);
  Global_THD_manager::get_instance()->do_for_all_thd_copy(&thd_timeout_checker);

  DBUG_VOID_RETURN;
}

/*
 Timer thread.

  Periodically, check if one of the thread groups is stalled. Stalls happen if
  events are not being dequeued from the queue, or from the network, Primary
  reason for stall can be a lengthy executing non-blocking request. It could
  also happen that thread is waiting but wait_begin/wait_end is forgotten by
  storage engine. Timer thread will create a new thread in group in case of
  a stall.

  Besides checking for stalls, timer thread is also responsible for terminating
  clients that have been idle for longer than wait_timeout seconds.

  TODO: Let the timer sleep for long time if there is no work to be done.
  Currently it wakes up rather often on and idle server.
*/

static void *timer_thread(void *param) noexcept {
  my_thread_init();
  DBUG_ENTER("timer_thread");

  pool_timer_t *timer = (pool_timer_t *)param;
  timer->next_timeout_check.store(ULLONG_MAX, std::memory_order_relaxed);
  timer->current_microtime.store(my_microsecond_getsystime(),
                                 std::memory_order_relaxed);

  for (;;) {
    struct timespec ts;

    set_timespec_nsec(&ts, timer->tick_interval * 1000000ULL);
    mysql_mutex_lock(&timer->mutex);
    int err = mysql_cond_timedwait(&timer->cond, &timer->mutex, &ts);
    if (timer->shutdown) {
      mysql_mutex_unlock(&timer->mutex);
      break;
    }
    if (err == ETIMEDOUT) {
      timer->current_microtime.store(my_microsecond_getsystime(),
                                     std::memory_order_relaxed);

      /* Check stalls in thread groups */
      for (size_t i = 0; i < array_elements(all_groups); i++) {
        if (all_groups[i].connection_count) check_stall(&all_groups[i]);
      }

      /* Check if any client exceeded wait_timeout */
      if (timer->next_timeout_check.load(std::memory_order_relaxed) <=
          timer->current_microtime.load(std::memory_order_relaxed))
        timeout_check(timer);
    }
    mysql_mutex_unlock(&timer->mutex);
  }

  mysql_mutex_destroy(&timer->mutex);
  my_thread_end();
  return NULL;
}

/*
  Check if both the high and low priority queues are empty.

  NOTE: we also consider the low priority queue empty in case it has events, but
  they cannot be processed due to the too_many_busy_threads() limit.
*/
static bool queues_are_empty(const thread_group_t &tg) noexcept {
  return (tg.high_prio_queue.is_empty() &&
          (tg.queue.is_empty() || too_many_busy_threads(tg)));
}

static void check_stall(thread_group_t *thread_group) {
  if (mysql_mutex_trylock(&thread_group->mutex) != 0) {
    /* Something happens. Don't disturb */
    return;
  }

  /*
    Check if listener is present. If not,  check whether any IO
    events were dequeued since last time. If not, this means
    listener is either in tight loop or thd_wait_begin()
    was forgotten. Create a new worker(it will make itself listener).
  */
  if (!thread_group->listener && !thread_group->io_event_count) {
    wake_or_create_thread(thread_group, true);
    mysql_mutex_unlock(&thread_group->mutex);
    return;
  }

  /*  Reset io event count */
  thread_group->io_event_count = 0;

  /*
    Check whether requests from the workqueues are being dequeued.

    The stall detection and resolution works as follows:

    1. There is a counter thread_group->queue_event_count for the number of
       events removed from the queues. Timer resets the counter to 0 on each
    run.
    2. Timer determines stall if this counter remains 0 since last check
       and at least one of the high and low priority queues is not empty.
    3. Once timer determined a stall it sets thread_group->stalled flag and
       wakes and idle worker (or creates a new one, subject to throttling).
    4. The stalled flag is reset, when an event is dequeued.

    Q : Will this handling lead to an unbound growth of threads, if queues
    stall permanently?
    A : No. If queues stall permanently, it is an indication for many very long
    simultaneous queries. The maximum number of simultanoues queries is
    max_connections, further we have threadpool_max_threads limit, upon which no
    worker threads are created. So in case there is a flood of very long
    queries, threadpool would slowly approach thread-per-connection behavior.
    NOTE:
    If long queries never wait, creation of the new threads is done by timer,
    so it is slower than in real thread-per-connection. However if long queries
    do wait and indicate that via thd_wait_begin/end callbacks, thread creation
    will be faster.
  */
  if (!thread_group->queue_event_count && !queues_are_empty(*thread_group)) {
    thread_group->stalled = true;
    TP_INCREMENT_GROUP_COUNTER(thread_group, stalls);
    wake_or_create_thread(thread_group, true);
  }

  /* Reset queue event count */
  thread_group->queue_event_count = 0;

  mysql_mutex_unlock(&thread_group->mutex);
}

static void start_timer(pool_timer_t *timer) noexcept {
  my_thread_handle thread_id;
  DBUG_ENTER("start_timer");
  mysql_mutex_init(key_timer_mutex, &timer->mutex, NULL);
  mysql_cond_init(key_timer_cond, &timer->cond);
  timer->shutdown = false;
  mysql_thread_create(key_timer_thread, &thread_id, NULL, timer_thread, timer);
  DBUG_VOID_RETURN;
}

static void stop_timer(pool_timer_t *timer) noexcept {
  DBUG_ENTER("stop_timer");
  mysql_mutex_lock(&timer->mutex);
  timer->shutdown = true;
  mysql_cond_signal(&timer->cond);
  mysql_mutex_unlock(&timer->mutex);
  DBUG_VOID_RETURN;
}
