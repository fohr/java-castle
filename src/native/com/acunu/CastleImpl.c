#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <netdb.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/select.h>
#include <linux/types.h>
#include <linux/netlink.h>

#include <jni.h>
#include <com_acunu_castle_Castle.h>
#include <com_acunu_castle_Key.h>

#include <com_acunu_castle_BigGetRequest.h>
#include <com_acunu_castle_BigPutRequest.h>
#include <com_acunu_castle_GetChunkRequest.h>
#include <com_acunu_castle_GetRequest.h>
#include <com_acunu_castle_IterFinishRequest.h>
#include <com_acunu_castle_IterNextRequest.h>
#include <com_acunu_castle_IterStartRequest.h>
#include <com_acunu_castle_PutChunkRequest.h>
#include <com_acunu_castle_RemoveRequest.h>
#include <com_acunu_castle_ReplaceRequest.h>

#include <castle/castle.h>

#define _ppstr(x) #x
#define ppstr(x) _ppstr(x)

//#define DEBUG
#ifdef DEBUG
#define debug printf
#else
#define debug(_f, ...)  ((void)0)
#endif

typedef struct s_callback_queue callback_queue;
void callback_queue_create(callback_queue** queue, unsigned long max_size);
void callback_queue_destroy(callback_queue* queue);

static jclass castle_class = NULL;
static jclass callback_class = NULL;
static jclass castle_exception_class = NULL;
static jclass key_class = NULL;
static jclass request_class = NULL;
static jclass request_response_class = NULL;

static jmethodID callback_run_method = NULL;
static jmethodID callback_seterr_method = NULL;
static jmethodID callback_setresponse_method = NULL;
static jmethodID exception_init_method = NULL;
static jmethodID key_init_method = NULL;
static jmethodID request_copyto_method = NULL;
static jmethodID response_init_method = NULL;

static jfieldID castle_connptr_field = NULL;
static jfieldID castle_cbqueueptr_field = NULL;

#define JNU_ThrowError(env, err, msg) _JNU_ThrowError(env, err, __FILE__ ":" ppstr(__LINE__) ": " msg)
static void
_JNU_ThrowError(JNIEnv *env, int err, char* msg)
{
    if (castle_exception_class != NULL) {
        jstring jmsg = (*env)->NewStringUTF(env, msg);
        jobject exception = (*env)->NewObject(env, castle_exception_class, exception_init_method, err, jmsg);
        (*env)->Throw(env, exception);
    }
}

JNIEXPORT void JNICALL
Java_com_acunu_castle_Castle_init_1jni(JNIEnv *env, jclass cls)
{
    castle_class = (*env)->FindClass(env, "com/acunu/castle/Castle");
    castle_class = (jclass)(*env)->NewGlobalRef(env, castle_class);

    castle_connptr_field = (*env)->GetFieldID(env, cls, "connectionJNIPointer", "J");
    castle_cbqueueptr_field = (*env)->GetFieldID(env, cls, "callbackQueueJNIPointer", "J");
}

JNIEXPORT void JNICALL
Java_com_acunu_castle_Callback_init_1jni(JNIEnv *env, jclass cls)
{
    callback_class = (*env)->FindClass(env, "com/acunu/castle/Callback");
    callback_class = (jclass)(*env)->NewGlobalRef(env, callback_class);

    callback_run_method = (*env)->GetMethodID(env, callback_class, "run", "()V");
    callback_setresponse_method = (*env)->GetMethodID(env, callback_class, "setResponse", "(Lcom/acunu/castle/RequestResponse;)V");
    callback_seterr_method = (*env)->GetMethodID(env, callback_class, "setErr", "(I)V");
}

JNIEXPORT void JNICALL
Java_com_acunu_castle_CastleException_init_1jni(JNIEnv *env, jclass cls)
{
    castle_exception_class = (*env)->FindClass(env, "com/acunu/castle/CastleException");
    castle_exception_class = (jclass)(*env)->NewGlobalRef(env, castle_exception_class);

    exception_init_method = (*env)->GetMethodID(env, castle_exception_class, "<init>", "(ILjava/lang/String;)V");
}

JNIEXPORT void JNICALL
Java_com_acunu_castle_Key_init_1jni(JNIEnv *env, jclass cls)
{
    key_class = (*env)->FindClass(env, "com/acunu/castle/Key");
    key_class = (jclass)(*env)->NewGlobalRef(env, key_class);

    key_init_method = (*env)->GetMethodID(env, key_class, "<init>", "([[B)V");
}

JNIEXPORT void JNICALL
Java_com_acunu_castle_Request_init_1jni(JNIEnv *env, jclass cls)
{
    request_class = (*env)->FindClass(env, "com/acunu/castle/Request");
    request_class = (jclass)(*env)->NewGlobalRef(env, request_class);

    request_copyto_method = (*env)->GetMethodID(env, request_class, "copy_to", "(JI)V");
}

JNIEXPORT void JNICALL
Java_com_acunu_castle_RequestResponse_init_1jni(JNIEnv *env, jclass cls)
{
    request_response_class = (*env)->FindClass(env, "com/acunu/castle/RequestResponse");
    request_response_class = (jclass)(*env)->NewGlobalRef(env, request_response_class);

    response_init_method = (*env)->GetMethodID(env, request_response_class, "<init>", "(ZJJ)V");
}

JNIEXPORT void JNICALL
Java_com_acunu_castle_Castle_castle_1connect(JNIEnv *env, jobject obj)
{
    castle_connection *conn;
    int ret;

    if (!castle_connptr_field)
    {
        JNU_ThrowError(env, -EINVAL, "no connptr_field");
        return;
    }

    (*env)->SetLongField(env, obj, castle_connptr_field, (jlong) NULL);

    ret = castle_connect(&conn);
    if (ret)
    {
        JNU_ThrowError(env, ret, "connect");
        return;
    }

    (*env)->SetLongField(env, obj, castle_connptr_field, (jlong) conn);

    if (!castle_cbqueueptr_field)
    {
        JNU_ThrowError(env, -EINVAL, "no cbqueueptr_field");
        return;
    }

    (*env)->SetLongField(env, obj, castle_cbqueueptr_field, (jlong)NULL);

    callback_queue* queue = NULL;
    callback_queue_create(&queue, 1024);
    if (!queue)
    {
        JNU_ThrowError(env, -ENOMEM, "memory");
        return;
    }

    (*env)->SetLongField(env, obj, castle_cbqueueptr_field, (jlong)queue);

    return;
}

JNIEXPORT void JNICALL
Java_com_acunu_castle_Castle_castle_1disconnect(JNIEnv *env, jobject connection)
{
    castle_connection* conn = NULL;
    callback_queue* queue = NULL;

    assert(castle_connptr_field != NULL);
    assert(castle_cbqueueptr_field != NULL);

    queue = (callback_queue*)(*env)->GetLongField(env, connection, castle_cbqueueptr_field);
    if (queue)
        callback_queue_destroy(queue);
    (*env)->SetLongField(env, connection, castle_cbqueueptr_field, 0);

    conn = (castle_connection*)(*env)->GetLongField(env, connection, castle_connptr_field);

    if (conn != NULL)
      castle_disconnect(conn);

    return;
}

JNIEXPORT void JNICALL
Java_com_acunu_castle_Castle_castle_1free(JNIEnv *env, jobject connection)
{
    castle_connection *conn;

    assert(castle_connptr_field != NULL);

    conn = (castle_connection *)(*env)->GetLongField(env, connection, castle_connptr_field);

    (*env)->SetLongField(env, connection, castle_connptr_field, 0);

    if (conn != NULL)
      castle_free(conn);

    return;
}

JNIEXPORT jint JNICALL
Java_com_acunu_castle_Castle_castle_1merge_1start(JNIEnv       *env,
                                                  jobject       connection,
                                                  jintArray     array_list,
                                                  jlongArray    data_ext_list,
                                                  jint          metadata_ext_type,
                                                  jint          data_ext_type,
                                                  jint          bandwidth)
{
    castle_connection *conn;
    c_merge_cfg_t merge_cfg;
    c_merge_id_t merge_id = INVAL_MERGE_ID;
    int ret =0;

    assert(castle_connptr_field != NULL);

    conn = (castle_connection *)(*env)->GetLongField(env, connection, castle_connptr_field);
    if (!conn)
    {
        ret = -EINVAL;
        goto err_out;
    }

    merge_cfg.nr_arrays             = (*env)->GetArrayLength(env, array_list);
    merge_cfg.arrays                = (c_merge_id_t *)(*env)->GetIntArrayElements(env, array_list, 0);

    if (data_ext_list == NULL)
    {
        merge_cfg.nr_data_exts      = MERGE_ALL_DATA_EXTS;
        merge_cfg.data_exts         = NULL;
    }
    else
    {
        merge_cfg.nr_data_exts      = (*env)->GetArrayLength(env, data_ext_list);
        merge_cfg.data_exts         = (c_data_ext_id_t *)(*env)->GetLongArrayElements(env, data_ext_list, 0);
    }

    merge_cfg.metadata_ext_type     = metadata_ext_type;
    merge_cfg.data_ext_type         = data_ext_type;
    merge_cfg.bandwidth             = bandwidth;

    ret = castle_merge_start(conn, merge_cfg, &merge_id);

    (*env)->ReleaseIntArrayElements(env, array_list, (jint *)merge_cfg.arrays, 0);
	if (data_ext_list != NULL)
        (*env)->ReleaseLongArrayElements(env, data_ext_list, (jlong *)merge_cfg.data_exts, 0);

err_out:
    if (ret)
        JNU_ThrowError(env, ret, "merge_start");

    return merge_id;
}

JNIEXPORT void JNICALL Java_com_acunu_castle_gn_CastleEventsThread_events_1callback_1thread_1run
    (JNIEnv* env, jobject obj)
{
    int udev_exit = 0;
    struct sockaddr_un saddr;
    socklen_t addrlen;
    const int feature_on = 1;
    int retval;
    int sock;
    jclass obj_class;
    jmethodID callback_event_method;

    obj_class = (*env)->GetObjectClass(env, obj);
    callback_event_method = (*env)->GetMethodID(env,
                                                obj_class,
                                                "udevEvent",
                                                "(Ljava/lang/String;)V");
    if (callback_event_method == NULL)
    {
        JNU_ThrowError(env, -EINVAL, "events_callback_thread_run");
        return;
    }

    memset(&saddr, 0x00, sizeof(saddr));
    saddr.sun_family = AF_LOCAL;
    /* use abstract namespace for socket path */
    strcpy(&saddr.sun_path[1], "/org/kernel/udev/monitor");
    addrlen = offsetof(struct sockaddr_un, sun_path) + strlen(saddr.sun_path+1) + 1;

    sock = socket(AF_LOCAL, SOCK_DGRAM, 0);
    if (sock == -1) {
        JNU_ThrowError(env, errno, "events_callback_thread_run");
        fprintf(stderr, "error getting socket: %s\n", strerror(errno));
        return;
    }

    /* the bind takes care of ensuring only one copy running */
    retval = bind(sock, (struct sockaddr *) &saddr, addrlen);
    if (retval < 0) {
        JNU_ThrowError(env, errno, "events_callback_thread_run");
        fprintf(stderr, "bind failed: %s\n", strerror(errno));
        close(sock);
        return;
    }

    /* enable receiving of the sender credentials */
    setsockopt(sock, SOL_SOCKET, SO_PASSCRED, &feature_on, sizeof(feature_on));

    while (!udev_exit)
    {
        static char buf[2048*2];
        static char ret_buf[2048*2];
        ssize_t buflen;
        int fdcount;
        fd_set readfds;
        size_t bufpos;
        int copy;

        buflen = 0;
        FD_ZERO(&readfds);
        if (sock > 0)
            FD_SET(sock, &readfds);

        fdcount = select(sock+1, &readfds, NULL, NULL, NULL);
        if (fdcount < 0) {
            if (errno != EINTR)
            {
                fprintf(stderr, "error receiving uevent message: %s\n", strerror(errno));
                JNU_ThrowError(env, errno, "events_callback_thread_run");
                goto err_out;
            }
            continue;
        }

        buflen = 0;
        if ((sock > 0) && FD_ISSET(sock, &readfds)) {
            buflen = recv(sock, &buf, sizeof(buf), 0);
            if (buflen <=  0) {
                fprintf(stderr, "error receiving udev message: %s\n", strerror(errno));
                udev_exit = 1;
                JNU_ThrowError(env, errno, "events_callback_thread_run");
                goto err_out;
            }
        }

        if (buflen == 0)
            continue;

        ret_buf[0] = '\0';

        /* start of payload */
        bufpos = strlen(buf) + 1;
        copy = 0;
        while (bufpos < (size_t)buflen) {
            int keylen;
            char *key;

            key = &buf[bufpos];
            keylen = strlen(key);
            if (keylen == 0)
                break;

            if (!strncmp(key, "CMD", 3) && (!strncmp(key+4, "131", 3) || !strncmp(key+4, "132", 3)))
                    copy = 1;

            if (copy)
            {
                strcat(ret_buf, key);
                strcat(ret_buf, ":");
            }

            bufpos += keylen + 1;
        }

        if (strlen(ret_buf))
        {
		printf("Sending Event to Java: %s\n", ret_buf);
            (*env)->CallVoidMethod(env,
                                   obj,
                                   callback_event_method,
                                   (*env)->NewStringUTF(env, ret_buf));
            if ((*env)->ExceptionOccurred(env))
            {
                (*env)->ExceptionClear(env);
                break;
            }
        }
    }

err_out:
    close(sock);
    return;
}


/* Data path */

JNIEXPORT jobject JNICALL
Java_com_acunu_castle_Castle_castle_1buffer_1create(JNIEnv *env, jobject connection, jlong size)
{
    castle_connection *conn;
    int ret;
    char *buf = NULL;

    assert(castle_connptr_field != NULL);

    conn = (castle_connection *)(*env)->GetLongField(env, connection, castle_connptr_field);
    if (!conn)
      return NULL;

    ret = castle_shared_buffer_create(conn, &buf, size);
    if (ret || !buf)
    {
        JNU_ThrowError(env, ret, "buffer_create");
        return NULL;
    }

    return (*env)->NewDirectByteBuffer(env, buf, size);
}

JNIEXPORT void JNICALL
Java_com_acunu_castle_Castle_castle_1buffer_1destroy(JNIEnv *env, jobject connection, jobject buffer)
{
    castle_connection *conn;
    int ret;
    jlong size;
    char *buf;

    assert(castle_connptr_field != NULL);

    size = (*env)->GetDirectBufferCapacity(env, buffer);
    if (size < 0)
    {
        JNU_ThrowError(env, 0, "buffer_destroy: not a direct buffer");
        return;
    }

    conn = (castle_connection *)(*env)->GetLongField(env, connection, castle_connptr_field);
    if (!conn)
      return;

    buf = (*env)->GetDirectBufferAddress(env, buffer);
    if (!buf)
    {
        JNU_ThrowError(env, 0, "buffer_destroy: not a direct buffer");
        return;
    }

    ret = castle_shared_buffer_destroy(conn, buf, size);
    if (ret)
    {
        JNU_ThrowError(env, ret, "buffer_destroy: failed");
        return;
    }

    return;
}

JNIEXPORT jint JNICALL Java_com_acunu_castle_Key_length(JNIEnv *env, jclass cls, jobjectArray key) {
  /* Does not throw */
  int dims = (*env)->GetArrayLength(env, key);
  int lens[dims];
  int i;

  for (i = 0; i < dims; i++) {
    /* May throw ArrayIndexOutOfBoundsException */
    jobject subkey = (*env)->GetObjectArrayElement(env, key, i);
    if ((*env)->ExceptionOccurred(env))
        return -1;

    /* Does not throw */
    lens[i] = (*env)->GetArrayLength(env, subkey);
  }

  return castle_key_bytes_needed(dims, lens, NULL, NULL);
}

static int get_buffer(JNIEnv* env, jobject buffer, char** buf_out, jlong* len_out)
{
    char* buf = (*env)->GetDirectBufferAddress(env, buffer);
    if (!buf)
    {
        JNU_ThrowError(env, -EINVAL, "Invalid buffer");
        return -EINVAL;
    }

    jlong len = (*env)->GetDirectBufferCapacity(env, buffer);
    if (len < 0)
    {
        JNU_ThrowError(env, -EINVAL, "Invalid buffer length");
        return -EINVAL;
    }

    *buf_out = buf;
    *len_out = len;
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_acunu_castle_Request_alloc(JNIEnv* env, jclass cls, jint num)
{
    castle_request* req = malloc(num * sizeof(*req));
    if (!req)
        JNU_ThrowError(env, -ENOMEM, "Failed to allocate requests");
    return (jlong)req;
}

JNIEXPORT void JNICALL Java_com_acunu_castle_Request_free(JNIEnv* env, jclass cls, jlong req)
{
    castle_request* r = (castle_request*)req;
    free(r);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_ReplaceRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint index, jint collection,
                                                                     jobject keyBuffer, jint keyOffset, jint keyLength,
                                                                     jobject valueBuffer, jint valueOffset, jint valueLength) {
  castle_request *req = (castle_request *)buffer;

  char *key_buf = NULL;
  jlong key_buf_len = 0;
  if (0 != get_buffer(env, keyBuffer, &key_buf, &key_buf_len))
    return;

  char *value_buf = NULL;
  jlong value_buf_len = 0;
  if (0 != get_buffer(env, valueBuffer, &value_buf, &value_buf_len))
    return;

  assert(keyLength <= key_buf_len - keyOffset);
  assert(valueLength <= value_buf_len - valueOffset);

  castle_replace_prepare(req + index, collection,
                         (castle_key *) (key_buf + keyOffset), keyLength,
                         value_buf + valueOffset, valueLength, CASTLE_RING_FLAG_NONE);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_RemoveRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint index, jint collection,
                                                                    jobject keyBuffer, jint keyOffset, jint keyLength) {
  castle_request *req = (castle_request *)buffer;

  char *key_buf = NULL;
  jlong key_buf_len = 0;
  if (0 != get_buffer(env, keyBuffer, &key_buf, &key_buf_len))
    return;

  assert(keyLength <= key_buf_len - keyOffset);

  castle_remove_prepare(req + index, collection,
                        (castle_key *) (key_buf + keyOffset), keyLength, CASTLE_RING_FLAG_NONE);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_GetRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint index, jint collection,
                                                                 jobject keyBuffer, jint keyOffset, jint keyLength,
                                                                 jobject valueBuffer, jint valueOffset, jint valueLength) {

  castle_request *req = (castle_request *)buffer;

  char *key_buf = NULL;
  jlong key_buf_len = 0;
  if (0 != get_buffer(env, keyBuffer, &key_buf, &key_buf_len))
    return;

  char *value_buf = NULL;
  jlong value_buf_len = 0;
  if (0 != get_buffer(env, valueBuffer, &value_buf, &value_buf_len))
    return;

  assert(keyLength <= key_buf_len - keyOffset);
  assert(valueLength <= value_buf_len - valueOffset);

  castle_get_prepare(req + index, collection,
                     (castle_key *) (key_buf + keyOffset), keyLength,
                     value_buf + valueOffset, valueLength, CASTLE_RING_FLAG_NONE);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_CounterGetRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint index, jint collection,
                                                                        jobject keyBuffer, jint keyOffset, jint keyLength,
                                                                        jobject valueBuffer, jint valueOffset, jint valueLength) {
  castle_request *req = (castle_request *)buffer;

  char *key_buf = NULL;
  jlong key_buf_len = 0;
  if (0 != get_buffer(env, keyBuffer, &key_buf, &key_buf_len))
    return;

  char *value_buf = NULL;
  jlong value_buf_len = 0;
  if (0 != get_buffer(env, valueBuffer, &value_buf, &value_buf_len))
    return;

  assert(keyLength <= key_buf_len - keyOffset);
  assert(valueLength <= value_buf_len - valueOffset);

  castle_get_prepare(req + index, collection,
                     (castle_key *) (key_buf + keyOffset), keyLength,
                     value_buf + valueOffset, valueLength, CASTLE_RING_FLAG_NONE);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_CounterAddRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint index, jint collection,
                                                                        jobject keyBuffer, jint keyOffset, jint keyLength,
                                                                        jobject valueBuffer, jint valueOffset, jint valueLength) {
  castle_request *req = (castle_request *)buffer;

  char *key_buf = NULL;
  jlong key_buf_len = 0;
  if (0 != get_buffer(env, keyBuffer, &key_buf, &key_buf_len))
    return;

  char *value_buf = NULL;
  jlong value_buf_len = 0;
  if (0 != get_buffer(env, valueBuffer, &value_buf, &value_buf_len))
    return;

  assert(keyLength <= key_buf_len - keyOffset);
  assert(valueLength <= value_buf_len - valueOffset);

  castle_counter_add_replace_prepare(req + index, collection,
                                     (castle_key *) (key_buf + keyOffset), keyLength,
                                     value_buf + valueOffset, valueLength, CASTLE_RING_FLAG_NONE);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_CounterSetRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint index, jint collection,
                                                                        jobject keyBuffer, jint keyOffset, jint keyLength,
                                                                        jobject valueBuffer, jint valueOffset, jint valueLength) {
  castle_request *req = (castle_request *)buffer;

  char *key_buf = NULL;
  jlong key_buf_len = 0;
  if (0 != get_buffer(env, keyBuffer, &key_buf, &key_buf_len))
    return;

  char *value_buf = NULL;
  jlong value_buf_len = 0;
  if (0 != get_buffer(env, valueBuffer, &value_buf, &value_buf_len))
    return;

  assert(keyLength <= key_buf_len - keyOffset);
  assert(valueLength <= value_buf_len - valueOffset);

  castle_counter_set_replace_prepare(req + index, collection,
                                     (castle_key *) (key_buf + keyOffset), keyLength,
                                     value_buf + valueOffset, valueLength, CASTLE_RING_FLAG_NONE);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_IterStartRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint index, jint collection,
                                                                       jobject startKeyBuffer, jint startKeyOffset, jint startKeyLength,
                                                                       jobject endKeyBuffer, jint endKeyOffset, jint endKeyLength,
                                                                       jobject bbuffer, jint bufferOffset, jint bufferLength,
                                                                       jlong flags) {
  castle_request *req = (castle_request *)buffer;

  char *start_key_buf = NULL;
  jlong start_key_buf_len = 0;
  if (0 != get_buffer(env, startKeyBuffer, &start_key_buf, &start_key_buf_len))
    return;

  char *end_key_buf = NULL;
  jlong end_key_buf_len = 0;
  if (0 != get_buffer(env, endKeyBuffer, &end_key_buf, &end_key_buf_len))
    return;

  char *buf = NULL;
  jlong buf_len = 0;
  if (0 != get_buffer(env, bbuffer, &buf, &buf_len))
    return;

  assert(startKeyLength <= start_key_buf_len - startKeyOffset);
  assert(endKeyLength <= end_key_buf_len - endKeyOffset);
  assert(bufferLength <= buf_len - bufferOffset);

  castle_iter_start_prepare(req + index, collection,
                            (castle_key *) (start_key_buf + startKeyOffset), startKeyLength,
                            (castle_key *) (end_key_buf + endKeyOffset), endKeyLength,
                            buf, bufferLength,
                            flags);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_IterNextRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint index, jlong token,
                                                                      jobject bbuffer, jint bufferOffset, jint bufferLength) {
  castle_request *req = (castle_request *)buffer;

  char *buf = NULL;
  jlong buf_len = 0;
  if (0 != get_buffer(env, bbuffer, &buf, &buf_len))
    return;

  assert(bufferLength <= buf_len - bufferOffset);

  castle_iter_next_prepare(req + index, token, buf, bufferLength, CASTLE_RING_FLAG_NONE);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_IterFinishRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint index, jlong token) {
  castle_request *req = (castle_request *)buffer;

  castle_iter_finish_prepare(req + index, token, CASTLE_RING_FLAG_NONE);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_BigPutRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint index, jint collection,
                                                                    jobject keyBuffer, jint keyOffset, jint keyLength, jlong valueLength) {
  castle_request *req = (castle_request *)buffer;

  char *key_buf = NULL;
  jlong key_buf_len = 0;
  if (0 != get_buffer(env, keyBuffer, &key_buf, &key_buf_len))
    return;

  assert(keyLength <= key_buf_len - keyOffset);

  castle_big_put_prepare(req + index, collection,
                         (castle_key *) (key_buf + keyOffset), keyLength,
                         valueLength, CASTLE_RING_FLAG_NONE);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_PutChunkRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint index, jlong token, jobject chunkBuffer, jint chunkOffset, jint chunkLength) {

  castle_request *req = (castle_request *)buffer;

  char *buf = NULL;
  jlong buf_len = 0;
  if (0 != get_buffer(env, chunkBuffer, &buf, &buf_len))
    return;

  assert(chunkLength <= buf_len - chunkOffset);

  castle_put_chunk_prepare(req + index, token, buf + chunkOffset, chunkLength, CASTLE_RING_FLAG_NONE);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_BigGetRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint index, jint collection,
                                                                    jobject keyBuffer, jint keyOffset, jint keyLength) {
  castle_request *req = (castle_request *)buffer;

  char *key_buf = NULL;
  jlong key_buf_len = 0;
  if (0 != get_buffer(env, keyBuffer, &key_buf, &key_buf_len))
    return;

  assert(keyLength <= key_buf_len - keyOffset);

  castle_big_get_prepare(req + index, collection,
                         (castle_key *) (key_buf + keyOffset), keyLength, CASTLE_RING_FLAG_NONE);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_GetChunkRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint index, jlong token, jobject chunkBuffer, jint chunkOffset, jint chunkLength) {

  castle_request *req = (castle_request *)buffer;

  char *buf = NULL;
  jlong buf_len = 0;
  if (0 != get_buffer(env, chunkBuffer, &buf, &buf_len))
    return;

  assert(chunkLength <= buf_len - chunkOffset);

  castle_get_chunk_prepare(req + index, token, buf + chunkOffset, chunkLength, CASTLE_RING_FLAG_NONE);
}

JNIEXPORT jlong JNICALL
Java_com_acunu_castle_Castle_castle_1get_1start_1address(JNIEnv *env, jobject connection, jobject buffer)
{
    return (jlong)(*env)->GetDirectBufferAddress(env, buffer);
}

JNIEXPORT jobject JNICALL
Java_com_acunu_castle_Castle_castle_1request_1blocking(JNIEnv *env, jobject connection, jlong request)
{
    castle_request_t* req = (castle_request_t*)request;
    castle_connection *conn;
    struct castle_blocking_call call;
    int ret;
    jobject response;
    jboolean found = JNI_TRUE;

    conn = (castle_connection *)(*env)->GetLongField(env, connection, castle_connptr_field);
    if (!conn)
      return NULL;

    ret = castle_request_do_blocking(conn, req, &call);
    if (ret == -ENOENT)
        found = JNI_FALSE;
    if (ret && ret != -ENOENT)
    {
        JNU_ThrowError(env, ret, "castle_request_blocking: castle_request_do_blocking failed");
        return NULL;
    }

    response = (*env)->NewObject(env, request_response_class, response_init_method, found, (jlong)call.length, (jlong)call.token);

    return response;
}

JNIEXPORT jobjectArray JNICALL
Java_com_acunu_castle_Castle_castle_1request_1blocking_1multi(JNIEnv *env, jobject connection, jlong request_array, jint request_count)
{
    castle_request_t* req = (castle_request_t*)request_array;
    castle_connection *conn;
    struct castle_blocking_call *call;
    int ret, i;
    jobject response;
    jobjectArray response_array;

    call = malloc(sizeof(struct castle_blocking_call) * request_count);
    if (!call)
    {
        JNU_ThrowError(env, -ENOMEM, "No memory to allocate calls");
        goto err0;
    }

    /* Does not throw */
    conn = (castle_connection *)(*env)->GetLongField(env, connection, castle_connptr_field);
    if (!conn)
      goto err1;

    ret = castle_request_do_blocking_multi(conn, req, call, request_count);
    /* if any failed, throw an exception now */
    if (ret && ret != -ENOENT)
    {
        JNU_ThrowError(env, ret, "castle_request_blocking: castle_request_do_blocking failed");
        goto err1;
    }

    /* build up response array */

    response_array = (*env)->NewObjectArray(env, request_count, request_response_class, NULL);
    if (!response_array || (*env)->ExceptionOccurred(env))
      goto err1;

    for (i = 0; i < request_count; i++)
    {
        jboolean found = (call[i].err != -ENOENT);
        response = (*env)->NewObject(env, request_response_class, response_init_method, found,
            (jlong)call[i].length, (jlong)call[i].token);
        if (!response || (*env)->ExceptionOccurred(env))
          goto err1;

        (*env)->SetObjectArrayElement(env, response_array, i, response);
        if ((*env)->ExceptionOccurred(env))
          goto err1;
    }

    free(call);

    return response_array;

err1: free(call);
err0: return NULL;
}

typedef struct
{
    jobject callback;
    callback_queue* queue;
} cb_userdata;

typedef struct s_callback_data
{
    jobject callback;
    castle_response resp;
    struct s_callback_data* next;
} callback_data;

struct s_callback_queue
{
    unsigned long max_size;

    callback_data* head;
    callback_data* tail;
    unsigned long size;

    pthread_mutex_t lock;
    pthread_cond_t push_cnd;
    pthread_cond_t pop_cnd;
};

void callback_queue_create(callback_queue** queue, unsigned long max_size)
{
    *queue = calloc(1, sizeof(**queue));
    (*queue)->max_size = max_size;
    pthread_mutex_init(&(*queue)->lock, NULL);
    pthread_cond_init(&(*queue)->push_cnd, NULL);
    pthread_cond_init(&(*queue)->pop_cnd, NULL);
}

void callback_queue_destroy(callback_queue* queue)
{
    pthread_cond_destroy(&queue->push_cnd);
    pthread_cond_destroy(&queue->pop_cnd);
    pthread_mutex_destroy(&queue->lock);
    free(queue);
}

void callback_queue_shutdown(callback_queue* queue)
{
    pthread_mutex_lock(&queue->lock);
    queue->max_size = 0;
    pthread_cond_broadcast(&queue->pop_cnd);
    pthread_cond_broadcast(&queue->push_cnd);
    while(queue->size)
        pthread_cond_wait(&queue->pop_cnd, &queue->lock);
    pthread_mutex_unlock(&queue->lock);
}

int callback_queue_push(callback_queue* queue, callback_data* data)
{
    pthread_mutex_lock(&queue->lock);
    while(queue->max_size && queue->size >= queue->max_size)
        pthread_cond_wait(&queue->pop_cnd, &queue->lock);

    if (!queue->max_size) /* shutdown */
    {
        pthread_mutex_unlock(&queue->lock);
        return -1;
    }

    callback_data* old_tail = queue->tail;
    queue->tail = data;
    if (old_tail)
        old_tail->next = data;
    else
        queue->head = data;

    ++queue->size;
    pthread_cond_signal(&queue->push_cnd);
    pthread_mutex_unlock(&queue->lock);
    return 0;
}

int callback_queue_pop(callback_queue* queue, callback_data** data)
{
    pthread_mutex_lock(&queue->lock);
    while(queue->max_size && !queue->head)
        pthread_cond_wait(&queue->push_cnd, &queue->lock);

    if (!queue->head) /* shutdown */
    {
        *data = NULL;
        pthread_mutex_unlock(&queue->lock);
        return -1;
    }

    *data = queue->head;
    callback_data* new_head = queue->head->next;
    queue->head = new_head;

    if (!new_head)
        queue->tail = NULL;

    --queue->size;
    pthread_cond_signal(&queue->pop_cnd);
    pthread_mutex_unlock(&queue->lock);
    return 0;
}

/* swallow exception and return */
#define CATCH_AND_EXIT(label)  do { \
if ((*env)->ExceptionOccurred(env)) { \
    (*env)->ExceptionClear(env); \
    goto label; \
}}while(0)

/* propagate exception and return */
#define NOCATCH_AND_EXIT(label)  do { \
if ((*env)->ExceptionOccurred(env)) { \
    goto label; \
}}while(0)

#define CHK_MEM(x, label) do { \
    if(!(x)) { \
        JNU_ThrowError(env, -ENOMEM, "Memory allocation failed"); \
        goto label; \
}} while(0)

#define CHK_RESULT(x, label) do { \
    if(!(x)) { \
        JNU_ThrowError(env, -EINVAL, "Unknown error"); \
        goto label; \
}} while(0)

JNIEXPORT void JNICALL Java_com_acunu_castle_Castle_callback_1queue_1shutdown
    (JNIEnv* env, jobject connection)
{
    callback_queue* queue = (callback_queue*)(*env)->GetLongField(env, connection, castle_cbqueueptr_field);
    if (queue)
      callback_queue_shutdown(queue);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_Castle_callback_1thread_1run
    (JNIEnv* env, jobject connection)
{
    callback_queue* queue = (callback_queue*)(*env)->GetLongField(env, connection, castle_cbqueueptr_field);
    callback_data* data = NULL;

    if (!queue)
      return;

    while(0 == callback_queue_pop(queue, &data))
    {
        if (!data)
            break;

        jboolean found = data->resp.err ? JNI_FALSE : JNI_TRUE;

        jobject response = (*env)->NewObject(env, request_response_class, response_init_method, found, (jlong)data->resp.length, (jlong)data->resp.token);
        CATCH_AND_EXIT(out1);
        if (!response)
            goto out1;

        (*env)->CallVoidMethod(env, data->callback, callback_setresponse_method, response);
        CATCH_AND_EXIT(out2);

        (*env)->CallVoidMethod(env, data->callback, callback_seterr_method, data->resp.err);
        CATCH_AND_EXIT(out2);

        (*env)->CallVoidMethod(env, data->callback, callback_run_method);
        CATCH_AND_EXIT(out2);

    out2: (*env)->DeleteLocalRef(env, response);
    out1: (*env)->DeleteGlobalRef(env, data->callback);
        free(data);
    }

    return;
}

void handle_callback(castle_connection* conn, castle_response* resp, void* userdata)
{
    cb_userdata* data = (cb_userdata*)userdata;
    if (!data)
        return;
    callback_data* node = (callback_data*)calloc(1, sizeof(*node));
    if (!node)
        return;

    node->callback = data->callback;
    memcpy(&node->resp, resp, sizeof(*resp));

    callback_queue_push(data->queue, node);
    free(data);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_Castle_castle_1request_1send_1multi
  (JNIEnv* env, jobject connection, jlong requests, jint num_requests, jobject callback)
{
    castle_connection* conn = NULL;
    callback_queue* queue = NULL;

    castle_request_t* reqs = (castle_request_t*)requests;
    cb_userdata* userdata = NULL;

    CHK_MEM( userdata = malloc(sizeof(*userdata)), ret);

    /* nothrow */
    conn = (castle_connection*)(*env)->GetLongField(env, connection, castle_connptr_field);
    CHK_RESULT(conn, err1);

    /* nothrow */
    queue = (callback_queue*)(*env)->GetLongField(env, connection, castle_cbqueueptr_field);
    CHK_RESULT(queue, err1);

    /* nothrow */
    userdata->callback = (*env)->NewGlobalRef(env, callback);
    CHK_RESULT(userdata->callback, err1);
    userdata->queue = queue;

    castle_request_send_batch(conn, reqs, &handle_callback, (void*)userdata, (int)num_requests);
    goto ret;

err1: free(userdata);
ret:  return;
}


/* IOCTLS */

/* Macros mapping java types to c types */

#define JNI_TYPE_slave_uuid jint
#define JNI_TYPE_collection_id jint
#define JNI_TYPE_version jint
#define JNI_TYPE_uint32 jint
#define JNI_TYPE_uint64 jlong
#define JNI_TYPE_size jint
#define JNI_TYPE_string const char *
#define JNI_TYPE_int32 jint
#define JNI_TYPE_da_id_t jint
#define JNI_TYPE_merge_id_t jint
#define JNI_TYPE_thread_id_t jint
#define JNI_TYPE_work_id_t jint
#define JNI_TYPE_work_size_t jlong

/* Macros to convert java types to c types */

#define JNI_CONV_slave_uuid(_j) _j
#define JNI_CONV_collection_id(_j) _j
#define JNI_CONV_version(_j) _j
#define JNI_CONV_uint32(_j) _j
#define JNI_CONV_uint64(_j) _j
#define JNI_CONV_size(_j) _j
#define JNI_CONV_string(_j) _j
#define JNI_CONV_int32(_j) _j
#define JNI_CONV_da_id_t(_j) _j
#define JNI_CONV_merge_id_t(_j) _j
#define JNI_CONV_thread_id_t(_j) _j
#define JNI_CONV_work_id_t(_j) _j
#define JNI_CONV_work_size_t(_j) _j

#define FUN_NAME_claim                  Java_com_acunu_castle_Castle_castle_1claim
#define FUN_NAME_release                Java_com_acunu_castle_Castle_castle_1release
#define FUN_NAME_attach                 Java_com_acunu_castle_Castle_castle_1attach
#define FUN_NAME_detach                 Java_com_acunu_castle_Castle_castle_1detach
#define FUN_NAME_snapshot               Java_com_acunu_castle_Castle_castle_1snapshot
#define FUN_NAME_collection             Java_com_acunu_castle_Castle_castle_1collection
#define FUN_NAME_create                 Java_com_acunu_castle_Castle_castle_1create
#define FUN_NAME_delete_version         Java_com_acunu_castle_Castle_castle_1delete_1version
#define FUN_NAME_destroy_vertree        Java_com_acunu_castle_Castle_castle_1destroy_1vertree
#define FUN_NAME_clone                  Java_com_acunu_castle_Castle_castle_1clone
#define FUN_NAME_init                   Java_com_acunu_castle_Castle_castle_1init
#define FUN_NAME_collection_attach      Java_com_acunu_castle_Castle_castle_1collection_1attach
#define FUN_NAME_collection_detach      Java_com_acunu_castle_Castle_castle_1collection_1detach
#define FUN_NAME_collection_snapshot    Java_com_acunu_castle_Castle_castle_1collection_1snapshot
#define FUN_NAME_merge_thread_create    Java_com_acunu_castle_Castle_castle_1merge_1thread_1create
#define FUN_NAME_merge_thread_destroy   Java_com_acunu_castle_Castle_castle_1merge_1thread_1destroy
#define FUN_NAME_merge_do_work          Java_com_acunu_castle_Castle_castle_1merge_1do_1work
#define FUN_NAME_merge_stop             Java_com_acunu_castle_Castle_castle_1merge_1stop
#define FUN_NAME_merge_thread_attach    Java_com_acunu_castle_Castle_castle_1merge_1thread_1attach
#define FUN_NAME_insert_rate_set        Java_com_acunu_castle_Castle_castle_1insert_1rate_1set


#define CASTLE_IOCTL_0IN_0OUT(_id, _name)                                                           \
JNIEXPORT void JNICALL                                                                              \
FUN_NAME_##_id                                                                                      \
(JNIEnv *env, jobject connection)                                                                   \
{                                                                                                   \
        castle_connection *conn;                                                       \
        int ret;                                                                                    \
                                                                                                    \
        conn = (castle_connection *)(*env)->GetLongField(env,                          \
            connection, castle_connptr_field);                                                            \
        if (!conn)                                                      \
          return;                                                  \
                                                                                                    \
        ret = castle_##_id(conn);                                                             \
                                                                                                    \
        if (ret)                                                                                    \
            JNU_ThrowError(env, ret, #_id);                                                         \
                                                                                                    \
        return;                                                                                     \
}                                                                                                   \

#define CASTLE_IOCTL_0IN_1OUT(_id, _name, _ret_t, _ret)                                             \
JNIEXPORT JNI_TYPE_##_ret_t JNICALL                                                                 \
FUN_NAME_##_id                                                                                      \
(JNIEnv *env, jobject connection)                                                                   \
{                                                                                                   \
        castle_connection *conn;                                                                    \
        int ret;                                                                                    \
        C_TYPE_##_ret_t _ret;                                                                       \
                                                                                                    \
        conn = (castle_connection *)(*env)->GetLongField(env,                                       \
            connection, castle_connptr_field);                                                      \
        if (!conn)                                                                                  \
          return 0;                                                                                 \
                                                                                                    \
        ret = castle_##_id(conn, &_ret);                                                            \
                                                                                                    \
        if (ret)                                                                                    \
            JNU_ThrowError(env, ret, #_id);                                                         \
                                                                                                    \
        return _ret;                                                                                \
}                                                                                                   \

#define CASTLE_IOCTL_1IN_0OUT(_id, _name, _arg_1_t, _arg_1)                                         \
JNIEXPORT void JNICALL                                                                              \
FUN_NAME_##_id                                                                                      \
(JNIEnv *env, jobject connection, JNI_TYPE_##_arg_1_t j##_arg_1)                                    \
{                                                                                                   \
        castle_connection *conn;                                                       \
        int ret;                                                                                    \
        C_TYPE_##_arg_1_t _arg_1 = JNI_CONV_##_arg_1_t(j##_arg_1);                                  \
                                                                                                    \
        conn = (castle_connection *)(*env)->GetLongField(env,                          \
            connection, castle_connptr_field);                                                            \
        if (!conn)                                                      \
          return;                                                  \
                                                                                                    \
        ret = castle_##_id(conn, _arg_1);                                                     \
                                                                                                    \
        if (ret)                                                                                    \
            JNU_ThrowError(env, ret, #_id);                                                         \
                                                                                                    \
        return;                                                                                     \
}                                                                                                   \

#define CASTLE_IOCTL_1IN_1OUT(_id, _name, _arg_1_t, _arg_1, _ret_t, _ret)                           \
JNIEXPORT JNI_TYPE_##_ret_t JNICALL                                                                 \
FUN_NAME_##_id                                                                                      \
(JNIEnv *env, jobject connection, JNI_TYPE_##_arg_1_t j##_arg_1)                                    \
{                                                                                                   \
        castle_connection *conn;                                                       \
        int ret;                                                                                    \
        C_TYPE_##_arg_1_t _arg_1 = JNI_CONV_##_arg_1_t(j##_arg_1);                                  \
        C_TYPE_##_ret_t _ret;                                                                       \
                                                                                                    \
        conn = (castle_connection *)(*env)->GetLongField(env,                          \
            connection, castle_connptr_field);                                                            \
        if (!conn)                                                      \
          return 0;                                                  \
                                                                                                    \
        ret = castle_##_id(conn, _arg_1, &_ret);                                              \
                                                                                                    \
        if (ret)                                                                                    \
            JNU_ThrowError(env, ret, #_id);                                                         \
                                                                                                    \
        return _ret;                                                                                \
}                                                                                                   \

#define CASTLE_IOCTL_2IN_0OUT(_id, _name, _arg_1_t, _arg_1, _arg_2_t, _arg_2)                       \
JNIEXPORT void JNICALL                                                                              \
FUN_NAME_##_id                                                                                      \
(JNIEnv *env, jobject connection, JNI_TYPE_##_arg_1_t j##_arg_1, JNI_TYPE_##_arg_2_t j##_arg_2)     \
{                                                                                                   \
        castle_connection *conn;                                                       \
        int ret;                                                                                    \
        C_TYPE_##_arg_1_t _arg_1 = JNI_CONV_##_arg_1_t(j##_arg_1);                                  \
        C_TYPE_##_arg_2_t _arg_2 = JNI_CONV_##_arg_2_t(j##_arg_2);                                  \
                                                                                                    \
        conn = (castle_connection *)(*env)->GetLongField(env,                          \
            connection, castle_connptr_field);                                                            \
        if (!conn)                                                      \
          return;                                                  \
                                                                                                    \
        ret = castle_##_id(conn, _arg_1, _arg_2);                                                     \
                                                                                                    \
        if (ret)                                                                                    \
            JNU_ThrowError(env, ret, #_id);                                                         \
                                                                                                    \
        return;                                                                                     \
}                                                                                                   \

#define CASTLE_IOCTL_2IN_1OUT(_id, _name, _arg_1_t, _arg_1, _arg_2_t, _arg_2, _ret_t, _ret)         \
JNIEXPORT JNI_TYPE_##_ret_t JNICALL                                                                 \
FUN_NAME_##_id                                                                                      \
(JNIEnv *env, jobject connection,                                                                   \
JNI_TYPE_##_arg_1_t j##_arg_1,                                                                      \
JNI_TYPE_##_arg_2_t j##_arg_2)                                                                      \
{                                                                                                   \
        castle_connection *conn;                                                                    \
        int ret;                                                                                    \
        C_TYPE_##_arg_1_t _arg_1 = JNI_CONV_##_arg_1_t(j##_arg_1);                                  \
        C_TYPE_##_arg_2_t _arg_2 = JNI_CONV_##_arg_2_t(j##_arg_2);                                  \
        C_TYPE_##_ret_t _ret;                                                                       \
                                                                                                    \
        conn = (castle_connection *)(*env)->GetLongField(env,                                       \
            connection, castle_connptr_field);                                                      \
        if (!conn)                                                                                  \
          return 0;                                                                                 \
                                                                                                    \
        ret = castle_##_id(conn, _arg_1, _arg_2, &_ret);                                            \
                                                                                                    \
        if (ret)                                                                                    \
            JNU_ThrowError(env, ret, #_id);                                                         \
                                                                                                    \
        return _ret;                                                                                \
}                                                                                                   \

#define CASTLE_IOCTL_3IN_1OUT(_id, _name, _arg_1_t, _arg_1, _arg_2_t, _arg_2,                       \
    _arg_3_t, _arg_3, _ret_t, _ret)                                                                 \
JNIEXPORT JNI_TYPE_##_ret_t JNICALL                                                                 \
FUN_NAME_##_id                                                                                      \
(JNIEnv *env, jobject connection,                                                                   \
JNI_TYPE_##_arg_1_t j##_arg_1,                                                                      \
JNI_TYPE_##_arg_2_t j##_arg_2,                                                                      \
JNI_TYPE_##_arg_3_t j##_arg_3)                                                                      \
{                                                                                                   \
        castle_connection *conn;                                                       \
        int ret;                                                                                    \
        C_TYPE_##_arg_1_t _arg_1 = JNI_CONV_##_arg_1_t(j##_arg_1);                                  \
        C_TYPE_##_arg_2_t _arg_2 = JNI_CONV_##_arg_2_t(j##_arg_2);                                  \
        C_TYPE_##_arg_3_t _arg_3 = JNI_CONV_##_arg_3_t(j##_arg_3);                                  \
        C_TYPE_##_ret_t _ret;                                                                       \
                                                                                                    \
        conn = (castle_connection *)(*env)->GetLongField(env,                          \
            connection, castle_connptr_field);                                                            \
        if (!conn)                                                      \
          return 0;                                                  \
                                                                                                    \
        ret = castle_##_id(conn, _arg_1, _arg_2, _arg_3, &_ret);                              \
                                                                                                    \
        if (ret)                                                                                    \
            JNU_ThrowError(env, ret, #_id);                                                         \
                                                                                                    \
        return _ret;                                                                                \
}                                                                                                   \

CASTLE_IOCTLS

JNIEXPORT jint JNICALL
Java_com_acunu_castle_Castle_castle_1collection_1attach_1str(JNIEnv *env, jobject connection, jint version, jstring name) {
  /* Does not throw */
  jsize name_len = (*env)->GetStringUTFLength(env, name) + 1; /* We have to include the null terminator */

  /* Does not throw */
  const char *name_str = (*env)->GetStringUTFChars(env, name, NULL);
  if (name_str == NULL) {
    return 0; /* OutOfMemoryError already thrown */
  }

  jint ret = Java_com_acunu_castle_Castle_castle_1collection_1attach(env, connection, version, name_str, name_len);

  /* Does not throw */
  (*env)->ReleaseStringUTFChars(env, name, name_str);
  return ret;
}
