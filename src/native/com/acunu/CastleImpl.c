#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <netdb.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>

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

#define JNU_ThrowError(env, err, msg) _JNU_ThrowError(env, err, __FILE__ ":" ppstr(__LINE__) ": " msg)
static void
_JNU_ThrowError(JNIEnv *env, int err, char* msg)
{
    jclass cls = (*env)->FindClass(env, "com/acunu/castle/CastleException");
    /* if cls is NULL, an exception has already been thrown */
    if (cls != NULL) {
        jstring jmsg = (*env)->NewStringUTF(env, msg);
        jmethodID constructor = (*env)->GetMethodID(env, cls, "<init>", "(ILjava/lang/String;)V");

        jobject exception = (*env)->NewObject(env, cls, constructor, err, jmsg);

        (*env)->Throw(env, exception);
    }
}

static jfieldID conn_ptr_field = NULL;
static jfieldID cbqueue_ptr_field = NULL;

JNIEXPORT void JNICALL
Java_com_acunu_castle_Castle_castle_1connect(JNIEnv *env, jobject obj)
{
    castle_connection *conn;
    int ret;

    /* Does not throw */
    jclass cls = (*env)->GetObjectClass(env, obj);

    if (!conn_ptr_field)
    {
        conn_ptr_field = (*env)->GetFieldID(env, cls, "connectionJNIPointer", "J");
        if (!conn_ptr_field || (*env)->ExceptionOccurred(env))
          return;
    }

    (*env)->SetLongField(env, obj, conn_ptr_field, (jlong) NULL);

    ret = castle_connect(&conn);
    if (ret)
    {
        JNU_ThrowError(env, ret, "connect");
        return;
    }

    (*env)->SetLongField(env, obj, conn_ptr_field, (jlong) conn);

    if (!cbqueue_ptr_field)
    {
        cbqueue_ptr_field = (*env)->GetFieldID(env, cls, "callbackQueueJNIPointer", "J");
        if (!cbqueue_ptr_field || (*env)->ExceptionOccurred(env))
            return;
    }

    (*env)->SetLongField(env, obj, cbqueue_ptr_field, (jlong)NULL);

    callback_queue* queue = NULL;
    callback_queue_create(&queue, 1024);
    if (!queue)
    {
        JNU_ThrowError(env, -ENOMEM, "memory");
        return;
    }

    (*env)->SetLongField(env, obj, cbqueue_ptr_field, (jlong)queue);

    return;
}

JNIEXPORT void JNICALL
Java_com_acunu_castle_Castle_castle_1disconnect(JNIEnv *env, jobject connection)
{
    castle_connection* conn = NULL;
    callback_queue* queue = NULL;

    assert(conn_ptr_field != NULL);
    assert(cbqueue_ptr_field != NULL);

    queue = (callback_queue*)(*env)->GetLongField(env, connection, cbqueue_ptr_field);
    if (queue)
        callback_queue_destroy(queue);
    (*env)->SetLongField(env, connection, cbqueue_ptr_field, 0);

    conn = (castle_connection*)(*env)->GetLongField(env, connection, conn_ptr_field);

    if (conn != NULL)
      castle_disconnect(conn);

    return;
}

JNIEXPORT void JNICALL
Java_com_acunu_castle_Castle_castle_1free(JNIEnv *env, jobject connection)
{
    castle_connection *conn;

    assert(conn_ptr_field != NULL);

    conn = (castle_connection *)(*env)->GetLongField(env, connection, conn_ptr_field);

    (*env)->SetLongField(env, connection, conn_ptr_field, 0);

    if (conn != NULL)
      castle_free(conn);

    return;
}

/* Data path */

JNIEXPORT jobject JNICALL
Java_com_acunu_castle_Castle_castle_1buffer_1create(JNIEnv *env, jobject connection, jlong size)
{
    castle_connection *conn;
    int ret;
    char *buf = NULL;

    assert(conn_ptr_field != NULL);

    conn = (castle_connection *)(*env)->GetLongField(env, connection, conn_ptr_field);
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

    assert(conn_ptr_field != NULL);

    size = (*env)->GetDirectBufferCapacity(env, buffer);
    if (size < 0)
    {
        JNU_ThrowError(env, 0, "buffer_destroy: not a direct buffer");
        return;
    }

    conn = (castle_connection *)(*env)->GetLongField(env, connection, conn_ptr_field);
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

  return castle_key_bytes_needed(dims, lens, NULL);
}

JNIEXPORT jint JNICALL Java_com_acunu_castle_Key_copy_1to(JNIEnv *env, jclass cls, jobjectArray key, jobject keyBuffer, jint keyOffset) {
  /* Does not throw */
  int dims = (*env)->GetArrayLength(env, key);
  int lens[dims];
  jarray subkeys[dims];
  jbyte *subkey_elems[dims];
  const uint8_t *keys[dims];
  int r;
  int i;

  /* Does not throw, just returns NULL */
  char *buf = (*env)->GetDirectBufferAddress(env, keyBuffer);
  if (!buf)
  {
    JNU_ThrowError(env, -EINVAL, "Not a valid buffer");
    return -1;
  }

  /* Does not throw */
  jlong buf_len = (*env)->GetDirectBufferCapacity(env, keyBuffer);
  if (buf_len <= 0)
  {
    JNU_ThrowError(env, -EINVAL, "Invalid buffer length");
    return -1;
  }

  /* Can never happen */
  assert(buf);
  assert(buf_len > 0);

  for (i = 0; i < dims; i++) {
    /* May throw ArrayIndexOutOfBoundsException */
    subkeys[i] = (*env)->GetObjectArrayElement(env, key, i);
    if ((*env)->ExceptionOccurred(env))
        return -1;

    /* Does not throw */
    lens[i] = (*env)->GetArrayLength(env, subkeys[i]);

    /* Does not throw, just returns NULL */
    subkey_elems[i] = (*env)->GetByteArrayElements(env, subkeys[i], NULL);
    keys[i] = (const uint8_t *)subkey_elems[i];

    assert(keys[i]);
  }

  if (keyOffset > buf_len)
    /* We can't even fit the start of the buffer in there */
    return castle_key_bytes_needed(dims, lens, NULL) + keyOffset;

  r = castle_build_key_len((castle_key *) (buf + keyOffset), buf_len - keyOffset, dims, lens, keys);

  for (i = 0; i < dims; i++)
    (*env)->ReleaseByteArrayElements(env, subkeys[i], subkey_elems[i], JNI_ABORT);

  return r;
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

JNIEXPORT void JNICALL Java_com_acunu_castle_ReplaceRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint collection,
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

  castle_replace_prepare(req, collection,
                         (castle_key *) (key_buf + keyOffset), keyLength,
                         value_buf + valueOffset, valueLength);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_RemoveRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint collection,
                                                                    jobject keyBuffer, jint keyOffset, jint keyLength) {
  castle_request *req = (castle_request *)buffer;

  char *key_buf = NULL;
  jlong key_buf_len = 0;
  if (0 != get_buffer(env, keyBuffer, &key_buf, &key_buf_len))
    return;
  
  assert(keyLength <= key_buf_len - keyOffset);

  castle_remove_prepare(req, collection,
                        (castle_key *) (key_buf + keyOffset), keyLength);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_GetRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint collection,
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

  castle_get_prepare(req, collection,
                     (castle_key *) (key_buf + keyOffset), keyLength,
                     value_buf + valueOffset, valueLength);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_IterStartRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint collection,
                                                                       jobject startKeyBuffer, jint startKeyOffset, jint startKeyLength,
                                                                       jobject endKeyBuffer, jint endKeyOffset, jint endKeyLength,
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

  assert(startKeyLength <= start_key_buf_len - startKeyOffset);
  assert(endKeyLength <= end_key_buf_len - endKeyOffset);

  castle_iter_start_prepare(req, collection,
                            (castle_key *) (start_key_buf + startKeyOffset), startKeyLength,
                            (castle_key *) (end_key_buf + endKeyOffset), endKeyLength,
                            flags);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_IterNextRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jlong token,
                                                                      jobject bbuffer, jint bufferOffset, jint bufferLength) {
  castle_request *req = (castle_request *)buffer;

  char *buf = NULL;
  jlong buf_len = 0;
  if (0 != get_buffer(env, bbuffer, &buf, &buf_len))
    return;

  assert(bufferLength <= buf_len - bufferOffset);

  castle_iter_next_prepare(req, token, buf, bufferLength);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_IterFinishRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jlong token) {
  castle_request *req = (castle_request *)buffer;

  castle_iter_finish_prepare(req, token);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_BigPutRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint collection,
                                                                    jobject keyBuffer, jint keyOffset, jint keyLength, jlong valueLength) {
  castle_request *req = (castle_request *)buffer;

  char *key_buf = NULL;
  jlong key_buf_len = 0;
  if (0 != get_buffer(env, keyBuffer, &key_buf, &key_buf_len))
    return;

  assert(keyLength <= key_buf_len - keyOffset);

  castle_big_put_prepare(req, collection,
                         (castle_key *) (key_buf + keyOffset), keyLength,
                         valueLength);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_PutChunkRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jlong token, jobject chunkBuffer, jint chunkOffset, jint chunkLength) {

  castle_request *req = (castle_request *)buffer;

  char *buf = NULL;
  jlong buf_len = 0;
  if (0 != get_buffer(env, chunkBuffer, &buf, &buf_len))
    return;

  assert(chunkLength <= buf_len - chunkOffset);

  castle_put_chunk_prepare(req, token, buf + chunkOffset, chunkLength);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_BigGetRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jint collection,
                                                                    jobject keyBuffer, jint keyOffset, jint keyLength) {
  castle_request *req = (castle_request *)buffer;

  char *key_buf = NULL;
  jlong key_buf_len = 0;
  if (0 != get_buffer(env, keyBuffer, &key_buf, &key_buf_len))
    return;

  assert(keyLength <= key_buf_len - keyOffset);

  castle_big_get_prepare(req, collection,
                         (castle_key *) (key_buf + keyOffset), keyLength);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_GetChunkRequest_copy_1to(JNIEnv *env, jclass cls, jlong buffer, jlong token, jobject chunkBuffer, jint chunkOffset, jint chunkLength) {

  castle_request *req = (castle_request *)buffer;

  char *buf = NULL;
  jlong buf_len = 0;
  if (0 != get_buffer(env, chunkBuffer, &buf, &buf_len))
    return;

  assert(chunkLength <= buf_len - chunkOffset);

  castle_get_chunk_prepare(req, token, buf + chunkOffset, chunkLength);
}

JNIEXPORT jobject JNICALL
Java_com_acunu_castle_Castle_castle_1get_1key(JNIEnv *env, jobject connection, jobject buffer)
{
    struct castle_key_value_list *kv_list;
    jobject key;
    jclass key_class;
    jmethodID key_constructor;
    jobjectArray key_array;
    uint32_t i;

    kv_list = (struct castle_key_value_list *)(*env)->GetDirectBufferAddress(env, buffer);

    if (kv_list == NULL)
    {
        JNU_ThrowError(env, 0, "NULL buffer");
        return NULL;
    }

    if (kv_list->key == NULL)
        return NULL;

    key_class = (*env)->FindClass(env, "com/acunu/castle/Key");
    if (!key_class || (*env)->ExceptionOccurred(env))
      return NULL;

    key_constructor = (*env)->GetMethodID(env, key_class, "<init>", "([[B)V");
    if (!key_constructor || (*env)->ExceptionOccurred(env))
      return NULL;

    key_array = (*env)->NewObjectArray(env, kv_list->key->nr_dims, (*env)->FindClass(env, "[B"), NULL);
    if (!key_array || (*env)->ExceptionOccurred(env))
      return NULL;

    for (i = 0; i < kv_list->key->nr_dims; i++)
    {
        jbyteArray dim;

        dim = (*env)->NewByteArray(env, kv_list->key->dims[i]->length);
        if (!dim || (*env)->ExceptionOccurred(env))
          return NULL;

        (*env)->SetByteArrayRegion(env, dim, 0, kv_list->key->dims[i]->length, (jbyte *)kv_list->key->dims[i]->key);
        if ((*env)->ExceptionOccurred(env))
          return NULL;

        (*env)->SetObjectArrayElement(env, key_array, i, dim);
        if ((*env)->ExceptionOccurred(env))
          return NULL;
    }

    key = (*env)->NewObject(env, key_class, key_constructor, key_array);
    if (!key || (*env)->ExceptionOccurred(env))
      return NULL;

    return key;
}

JNIEXPORT jobject JNICALL
Java_com_acunu_castle_Castle_castle_1get_1value(JNIEnv *env, jobject connection, jobject buffer)
{
    struct castle_key_value_list *kv_list;
    jobject val_buffer;

    kv_list = (struct castle_key_value_list *)(*env)->GetDirectBufferAddress(env, buffer);

    if (kv_list == NULL)
    {
        JNU_ThrowError(env, 0, "NULL buffer");
        return NULL;
    }

    if (kv_list->key == NULL)
    {
        JNU_ThrowError(env, 0, "castle_get_value called with no key");
        return NULL;
    }

    if (kv_list->val == NULL)
    {
        /* no values flag was set */
        return NULL;
    }

    if (!(kv_list->val->type & CVT_TYPE_INLINE))
        return NULL;

    val_buffer = (*env)->NewDirectByteBuffer(env, kv_list->val->val, kv_list->val->length);
    if (!val_buffer || (*env)->ExceptionOccurred(env))
      return NULL;

    return val_buffer;
}

JNIEXPORT jlong JNICALL
Java_com_acunu_castle_Castle_castle_1get_1value_1length(JNIEnv *env, jobject connection, jobject buffer)
{
    struct castle_key_value_list *kv_list;

    kv_list = (struct castle_key_value_list *)(*env)->GetDirectBufferAddress(env, buffer);

    if (kv_list == NULL)
    {
        JNU_ThrowError(env, 0, "NULL buffer");
        return 0;
    }

    if (kv_list->key == NULL)
    {
        JNU_ThrowError(env, 0, "castle_get_value_length called with no key");
        return 0;
    }

    if (kv_list->val == NULL)
    {
        /* it's unknown */
        return -1;
    }

    return kv_list->val->length;
}

JNIEXPORT jobject JNICALL
Java_com_acunu_castle_Castle_castle_1get_1next_1kv(JNIEnv *env, jobject connection, jobject buffer)
{
    struct castle_key_value_list *kv_list;
    jint orig_capacity;
    jmethodID get_capacity_method;
    jclass buffer_class;
    jobject next_buffer;

    kv_list = (struct castle_key_value_list *)(*env)->GetDirectBufferAddress(env, buffer);

    if (kv_list == NULL)
    {
        JNU_ThrowError(env, 0, "NULL buffer");
        return NULL;
    }

    if (kv_list->next == NULL)
        return NULL;

    buffer_class = (*env)->GetObjectClass(env, buffer);

    get_capacity_method = (*env)->GetMethodID(env, buffer_class, "capacity", "()I");
    if (!get_capacity_method || (*env)->ExceptionOccurred(env))
      return NULL;

    orig_capacity = (*env)->CallIntMethod(env, buffer, get_capacity_method);
    if ((*env)->ExceptionOccurred(env))
      return NULL;

    next_buffer = (*env)->NewDirectByteBuffer(env, kv_list->next, orig_capacity - ((long int)kv_list->next - (long int)kv_list));
    if (!next_buffer || (*env)->ExceptionOccurred(env))
      return NULL;

    return next_buffer;
}

JNIEXPORT jobject JNICALL
Java_com_acunu_castle_Castle_castle_1request_1blocking(JNIEnv *env, jobject connection, jobject request)
{
    castle_request_t req;
    castle_connection *conn;
    struct castle_blocking_call call;
    int ret;
    jclass request_class;
    jmethodID request_copy_to;
    jclass response_class;
    jobject response;
    jmethodID response_constructor;
    jboolean found = JNI_TRUE;

    conn = (castle_connection *)(*env)->GetLongField(env, connection, conn_ptr_field);
    if (!conn)
      return NULL;

    request_class = (*env)->FindClass(env, "com/acunu/castle/Request");
    if (!request_class || (*env)->ExceptionOccurred(env))
      return NULL;

    request_copy_to = (*env)->GetMethodID(env, request_class, "copy_to", "(J)V");
    if (!request_copy_to || (*env)->ExceptionOccurred(env))
      return NULL;

    response_class = (*env)->FindClass(env, "com/acunu/castle/RequestResponse");
    if (!response_class || (*env)->ExceptionOccurred(env))
      return NULL;

    response_constructor = (*env)->GetMethodID(env, response_class, "<init>", "(ZJJ)V");
    if (!response_constructor || (*env)->ExceptionOccurred(env))
      return NULL;

    (*env)->CallVoidMethod(env, request, request_copy_to, (jlong)&req);
    if ((*env)->ExceptionOccurred(env))
      return NULL;

    ret = castle_request_do_blocking(conn, &req, &call);
    if (ret == -ENOENT)
        found = JNI_FALSE;
    if (ret && ret != -ENOENT)
    {
        JNU_ThrowError(env, ret, "castle_request_blocking: castle_request_do_blocking failed");
        return NULL;
    }

    response = (*env)->NewObject(env, response_class, response_constructor, found, (jlong)call.length, (jlong)call.token);

    return response;
}

JNIEXPORT jobjectArray JNICALL
Java_com_acunu_castle_Castle_castle_1request_1blocking_1multi(JNIEnv *env, jobject connection, jobjectArray request_array)
{
    castle_request_t* req;
    castle_connection *conn;
    struct castle_blocking_call *call;
    int ret, request_count, i;
    jclass request_class;
    jmethodID request_copy_to;
    jclass response_class;
    jmethodID response_constructor;
    jobject response;
    jobjectArray response_array;

    request_class = (*env)->FindClass(env, "com/acunu/castle/Request");
    if (!request_class || (*env)->ExceptionOccurred(env))
      return NULL;

    request_copy_to = (*env)->GetMethodID(env, request_class, "copy_to", "(J)V");
    if (!request_copy_to || (*env)->ExceptionOccurred(env))
      return NULL;

    response_class = (*env)->FindClass(env, "com/acunu/castle/RequestResponse");
    if (!response_class || (*env)->ExceptionOccurred(env))
      return NULL;

    response_constructor = (*env)->GetMethodID(env, response_class, "<init>", "(ZJJ)V");
    if (!response_constructor || (*env)->ExceptionOccurred(env))
      return NULL;

    request_count = (*env)->GetArrayLength(env, request_array);

    req = malloc(sizeof(castle_request_t) * request_count);
    if (!req)
    {
        JNU_ThrowError(env, -ENOMEM, "No memory to allocate requests");
        goto err0;
    }

    call = malloc(sizeof(struct castle_blocking_call) * request_count);
    if (!call)
    {
        JNU_ThrowError(env, -ENOMEM, "No memory to allocate calls");
        goto err1;
    }

    for (i = 0; i < request_count; i++)
    {
      jobject request = (*env)->GetObjectArrayElement(env, request_array, i);
      if ((*env)->ExceptionOccurred(env))
        goto err2;

      (*env)->CallVoidMethod(env, request, request_copy_to, (jlong)&req[i]);
      if ((*env)->ExceptionOccurred(env))
        goto err2;
    }

    /* Does not throw */
    conn = (castle_connection *)(*env)->GetLongField(env, connection, conn_ptr_field);
    if (!conn)
      return NULL;

    ret = castle_request_do_blocking_multi(conn, req, call, request_count);
    /* if any failed, throw an exception now */
    if (ret && ret != -ENOENT)
    {
        JNU_ThrowError(env, ret, "castle_request_blocking: castle_request_do_blocking failed");
        goto err2;
    }

    /* build up response array */

    response_array = (*env)->NewObjectArray(env, request_count, response_class, NULL);
    if (!response_array || (*env)->ExceptionOccurred(env))
      goto err2;

    for (i = 0; i < request_count; i++)
    {
        jboolean found = (call[i].err != -ENOENT);
        response = (*env)->NewObject(env, response_class, response_constructor, found,
            (jlong)call[i].length, (jlong)call[i].token);
        if (!response || (*env)->ExceptionOccurred(env))
          goto err2;

        (*env)->SetObjectArrayElement(env, response_array, i, response);
        if ((*env)->ExceptionOccurred(env))
          goto err2;
    }

    free(req);
    free(call);

    return response_array;

err2: free(req);
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
    callback_queue* queue = (callback_queue*)(*env)->GetLongField(env, connection, cbqueue_ptr_field);
    if (queue)
      callback_queue_shutdown(queue);
}

JNIEXPORT void JNICALL Java_com_acunu_castle_Castle_callback_1thread_1run
    (JNIEnv* env, jobject connection)
{
    callback_queue* queue = (callback_queue*)(*env)->GetLongField(env, connection, cbqueue_ptr_field);
    callback_data* data = NULL;

    if (!queue)
      return;

    jclass callback_class = (*env)->FindClass(env, "com/acunu/castle/Callback");
    CATCH_AND_EXIT(err);
    if (!callback_class)
        goto err;

    jmethodID callback_setresponse = (*env)->GetMethodID(env, callback_class, "setResponse", "(Lcom/acunu/castle/RequestResponse;)V");
    CATCH_AND_EXIT(err);    
    if (!callback_setresponse)
        goto err;

    jmethodID callback_seterr = (*env)->GetMethodID(env, callback_class, "setErr", "(I)V");
    CATCH_AND_EXIT(err);
    if (!callback_seterr)
        goto err;

    jmethodID callback_run = (*env)->GetMethodID(env, callback_class, "run", "()V");
    CATCH_AND_EXIT(err);
    if (!callback_run)
        goto err;

    jclass response_class = (*env)->FindClass(env, "com/acunu/castle/RequestResponse");
    CATCH_AND_EXIT(err);    
    if (!response_class)
        goto err;

    jmethodID response_constructor = (*env)->GetMethodID(env, response_class, "<init>", "(ZJJ)V");
    CATCH_AND_EXIT(err);
    if (!response_constructor)
        goto err;

    while(0 == callback_queue_pop(queue, &data))
    {
        if (!data)
            break; 

        jboolean found = data->resp.err ? JNI_FALSE : JNI_TRUE;

        jobject response = (*env)->NewObject(env, response_class, response_constructor, found, (jlong)data->resp.length, (jlong)data->resp.token);
        CATCH_AND_EXIT(out1);
        if (!response)
            goto out1;

        (*env)->CallVoidMethod(env, data->callback, callback_setresponse, response);
        CATCH_AND_EXIT(out2);

        (*env)->CallVoidMethod(env, data->callback, callback_seterr, data->resp.err);
        CATCH_AND_EXIT(out2);

        (*env)->CallVoidMethod(env, data->callback, callback_run);
        CATCH_AND_EXIT(out2);

    out2: (*env)->DeleteLocalRef(env, response);
    out1: (*env)->DeleteGlobalRef(env, data->callback);
        free(data);
    }

err: return;
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
  (JNIEnv* env, jobject connection, jobjectArray requests, jobjectArray callbacks)
{
    size_t num_requests = (*env)->GetArrayLength(env, requests);
    if (num_requests != (*env)->GetArrayLength(env, callbacks))
    {
        JNU_ThrowError(env, -EINVAL, "Number of requests and callbacks must match");
        return;
    }

    castle_connection* conn = NULL;
    callback_queue* queue = NULL;

    castle_request_t* reqs = NULL;
    castle_callback* callback_fns = NULL;
    cb_userdata** userdata = NULL;

    CHK_MEM( reqs = calloc(num_requests, sizeof(*reqs)), ret );
    CHK_MEM( callback_fns = calloc(num_requests, sizeof(*callback_fns)), out0 );
    CHK_MEM( userdata = calloc(num_requests, sizeof(*userdata)), out1 );

    /* nothrow */
    conn = (castle_connection*)(*env)->GetLongField(env, connection, conn_ptr_field);
    CHK_RESULT(conn, out2);

    /* nothrow */
    queue = (callback_queue*)(*env)->GetLongField(env, connection, cbqueue_ptr_field);
    CHK_RESULT(queue, out2);

    jclass request_class = (*env)->FindClass(env, "com/acunu/castle/Request");
    NOCATCH_AND_EXIT(out2);
    CHK_RESULT(request_class, out2);

    jmethodID request_copy_to = (*env)->GetMethodID(env, request_class, "copy_to", "(J)V");
    NOCATCH_AND_EXIT(out2);
    CHK_RESULT(request_copy_to, out2);

    size_t i;
    for (i = 0; i < num_requests; ++i)
    {
        jobject request = (*env)->GetObjectArrayElement(env, requests, i);
        NOCATCH_AND_EXIT(out3);
        CHK_RESULT(request, out3);

        (*env)->CallVoidMethod(env, request, request_copy_to, (jlong)&reqs[i]);
        NOCATCH_AND_EXIT(out3);

        callback_fns[i] = &handle_callback;

        jobject callback = (*env)->GetObjectArrayElement(env, callbacks, i);
        NOCATCH_AND_EXIT(out3);
        CHK_RESULT(callback, out3);

        CHK_MEM( userdata[i] = calloc(1, sizeof(*userdata[i])), out3 );

        /* nothrow */
        userdata[i]->callback = (*env)->NewGlobalRef(env, callback);
        CHK_RESULT(userdata[i]->callback, out3);

        userdata[i]->queue = queue;
    }

    castle_request_send(conn, reqs, callback_fns, (void**)userdata, num_requests);
    goto out2;

out3:
    for (i = 0; i < num_requests; ++i )
    {
        if (userdata[i])
        {
            (*env)->DeleteGlobalRef(env, userdata[i]->callback);
            free(userdata[i]);
        }
    }
out2: free(userdata);
out1: free(callback_fns);
out0: free(reqs);
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


#define CASTLE_IOCTL_0IN_0OUT(_id, _name)                                                           \
JNIEXPORT void JNICALL                                                                              \
FUN_NAME_##_id                                                                                      \
(JNIEnv *env, jobject connection)                                                                   \
{                                                                                                   \
        castle_connection *conn;                                                       \
        int ret;                                                                                    \
                                                                                                    \
        conn = (castle_connection *)(*env)->GetLongField(env,                          \
            connection, conn_ptr_field);                                                            \
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
            connection, conn_ptr_field);                                                            \
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
            connection, conn_ptr_field);                                                            \
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
            connection, conn_ptr_field);                                                            \
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
            connection, conn_ptr_field);                                                            \
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
