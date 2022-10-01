
// This file is part of lmdb-js
// Copyright (c) 2013-2017 Timur Kristóf
// Licensed to you under the terms of the MIT license
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#ifndef NODE_LMDB_H
#define NODE_LMDB_H

#include <vector>
#include <unordered_map>
#include <algorithm>
#include <ctime>
#include <napi.h>
#include <node_api.h>

#include "splinterdb/transaction.h"
#include "splinterdb/public_util.h"
#include "lz4.h"

using namespace Napi;

// set the threshold of when to use shared buffers (for uncompressed entries larger than this value)
const size_t SHARED_BUFFER_THRESHOLD = 0x4000;
typedef int txn_t;
typedef int dbi_t;

#ifndef __CPTHREAD_H__
#define __CPTHREAD_H__

#ifdef _WIN32
# include <windows.h>
#else
# include <pthread.h>
#endif

#ifdef _WIN32
typedef CRITICAL_SECTION pthread_mutex_t;
typedef void pthread_mutexattr_t;
typedef void pthread_condattr_t;
typedef HANDLE pthread_t;
typedef CONDITION_VARIABLE pthread_cond_t;

#endif

#define NAPI_FUNCTION(name) napi_value name(napi_env env, napi_callback_info info)
#define ARGS(count) napi_value returnValue;\
	size_t argc = count;\
	napi_value args[count];\
	napi_get_cb_info(env, info, &argc, args, NULL, NULL);
#define GET_UINT32_ARG(target, position) napi_get_value_uint32(env, args[position], (uint32_t*) &target)
#define GET_INT64_ARG(position)\
    int64_t i64;\
    napi_get_value_int64(env, args[position], &i64);
#define RETURN_UINT32(value) { napi_create_uint32(env, value, &returnValue); return returnValue; }
#define RETURN_INT32(value) { napi_create_int32(env, value, &returnValue); return returnValue; }
#define RETURN_UNDEFINED { napi_get_undefined(env, &returnValue); return returnValue; }
#define THROW_ERROR(message) { napi_throw_error(env, NULL, message); napi_get_undefined(env, &returnValue); return returnValue; }
#define EXPORT_NAPI_FUNCTION(name, func) { napi_property_descriptor desc = { name, 0, func, 0, 0, 0, (napi_property_attributes) (napi_writable | napi_configurable), 0 };\
	napi_define_properties(env, exports, 1, &desc); }
#define EXPORT_FUNCTION_ADDRESS(name, func) { \
	napi_value address;\
	void* f = (void*) func;\
	napi_create_double(env, *((double*) &f), &address);\
	napi_property_descriptor desc = { name, 0, 0, 0, 0, address, (napi_property_attributes) (napi_writable | napi_configurable), 0 };\
	napi_define_properties(env, exports, 1, &desc); }

#ifdef _WIN32

int pthread_mutex_init(pthread_mutex_t *mutex, pthread_mutexattr_t *attr);
int pthread_mutex_destroy(pthread_mutex_t *mutex);
int pthread_mutex_lock(pthread_mutex_t *mutex);
int pthread_mutex_unlock(pthread_mutex_t *mutex);

int pthread_cond_init(pthread_cond_t *cond, pthread_condattr_t *attr);
int pthread_cond_destroy(pthread_cond_t *cond);
int pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex);
int pthread_cond_signal(pthread_cond_t *cond);
int pthread_cond_broadcast(pthread_cond_t *cond);

#endif

int cond_timedwait(pthread_cond_t *cond, pthread_mutex_t *mutex, uint64_t ns);

#endif /* __CPTHREAD_H__ */

class Logging {
  public:
	static int debugLogging;
	static int initLogging();
};

enum class LmdbKeyType {

	// Invalid key (used internally by lmdb-js)
	InvalidKey = -1,
	
	// Default key (used internally by lmdb-js)
	DefaultKey = 0,

	// UCS-2/UTF-16 with zero terminator - Appears to V8 as string
	StringKey = 1,
	
	// LMDB fixed size integer key with 32 bit keys - Appearts to V8 as an Uint32
	Uint32Key = 2,
	
	// LMDB default key format - Appears to V8 as node::Buffer
	BinaryKey = 3,

};
enum class KeyCreation {
	Reset = 0,
	Continue = 1,
	InArray = 2,
};
const int THEAD_MEMORY_THRESHOLD = 4000;
#define USER_MAX_KEY_SIZE ((int)100)

class TxnWrap;
class EnvWrap;
class CursorWrap;
class Compression;

// Exports misc stuff to the module
void setupExportMisc(Env env, Object exports);

// Helper callback
typedef void (*argtokey_callback_t)(slice &key);

void consoleLog(Value val);
void consoleLog(const char *msg);
void consoleLogN(int n);
void setFlagFromValue(int *flags, int flag, const char *name, bool defaultValue, Object options);
void writeValueToEntry(const Value &str, slice *val);
LmdbKeyType keyTypeFromOptions(const Value &val, LmdbKeyType defaultKeyType = LmdbKeyType::DefaultKey);
int getVersionAndUncompress(slice &data, EnvWrap* ew);
int compareFast(const slice *a, const slice *b);
Value setGlobalBuffer(const CallbackInfo& info);
Value lmdbError(const CallbackInfo& info);
napi_value createBufferForAddress(napi_env env, napi_callback_info info);
napi_value getViewAddress(napi_env env, napi_callback_info info);
napi_value detachBuffer(napi_env env, napi_callback_info info);
Value getAddress(const CallbackInfo& info);
Value lmdbNativeFunctions(const CallbackInfo& info);
Value enableDirectV8(const CallbackInfo& info);

#ifndef thread_local
#ifdef __GNUC__
# define thread_local __thread
#elif __STDC_VERSION__ >= 201112L
# define thread_local _Thread_local
#elif defined(_MSC_VER)
# define thread_local __declspec(thread)
#else
# define thread_local
#endif
#endif

bool valToBinaryFast(slice &data, EnvWrap* ew);
Value valToUtf8(Env env, slice &data);
Value valToString(slice &data);
Value valToStringUnsafe(slice &data);
Value valToBinary(slice &data);
Value valToBinaryUnsafe(slice &data, EnvWrap* ew, Env env);

int putWithVersion(txn_t* txn,
		slice *   key,
		slice *   data,
		unsigned int	flags, double version);

Napi::Value throwLmdbError(Napi::Env env, int rc);
Napi::Value throwError(Napi::Env env, const char* message);

class TxnWrap;
class EnvWrap;
class CursorWrap;
class SharedEnv {
  public:
	splinterdb* env;
	uint64_t dev;
	uint64_t inode;
	int count;
};

const int INTERRUPT_BATCH = 9998;
const int WORKER_WAITING = 9997;
const int RESTART_WORKER_TXN = 9999;
const int RESUME_BATCH = 9996;
const int USER_HAS_LOCK = 9995;
const int SEPARATE_FLUSHED = 1;
const int DELETE_ON_CLOSE = 2;

class WriteWorker {
  public:
	WriteWorker(splinterdb* env, EnvWrap* envForTxn, uint32_t* instructions);
	void Write();
	txn_t* txn;
	txn_t* AcquireTxn(int* flags);
	void UnlockTxn();
	int WaitForCallbacks(txn_t** txn, bool allowCommit, uint32_t* target);
	virtual void ReportError(const char* error);
	virtual void SendUpdate();
	int interruptionStatus;
	bool finishedProgress;
	bool hasError;
	EnvWrap* envForTxn;
	virtual ~WriteWorker();
	uint32_t* instructions;
	int progressStatus;
	splinterdb* env;
	static int DoWrites(txn_t* txn, EnvWrap* envForTxn, uint32_t* instruction, WriteWorker* worker);
};
class AsyncWriteWorker : public WriteWorker, public AsyncProgressWorker<char> {
  public:
	AsyncWriteWorker(splinterdb* env, EnvWrap* envForTxn, uint32_t* instructions, const Function& callback);
	void Execute(const AsyncProgressWorker::ExecutionProgress& execution);
	void OnProgress(const char* data, size_t count);
	void OnOK();
	void ReportError(const char* error);
	void SendUpdate();
  private:
	ExecutionProgress* executionProgress;
};
class TxnTracked {
  public:
	TxnTracked(txn_t *txn, unsigned int flags);
	~TxnTracked();
	unsigned int flags;
	txn_t *txn;
	TxnTracked *parent;
};


typedef void* (get_shared_buffers_t)();
/*
	`Env`
	Represents a database environment.
	(Wrapper for `splinterdb`)
*/
typedef struct env_tracking_t {
	pthread_mutex_t* envsLock;
	std::vector<SharedEnv> envs;
	get_shared_buffers_t* getSharedBuffers;
} env_tracking_t;

typedef struct buffer_info_t {
	uint32_t id;
	size_t end;
	splinterdb* env;
	napi_ref ref;
} buffer_info_t;
class EnvWrap : public ObjectWrap<EnvWrap> {
private:
	// List of open read transactions
	std::vector<TxnWrap*> readTxns;
	static env_tracking_t* initTracking();
	napi_env napiEnv;
	// compression settings and space
	Compression *compression;
	static thread_local std::vector<EnvWrap*>* openEnvWraps;

	// Cleans up stray transactions
	void cleanupStrayTxns();
	void consolidateTxns();
   static void cleanupEnvWraps(void* data);

	friend class TxnWrap;

public:
	EnvWrap(const CallbackInfo&);
	~EnvWrap();
	// The wrapped object
	splinterdb *env;
	// Current write transaction
	static thread_local std::unordered_map<void*, buffer_info_t>* sharedBuffers;
	static env_tracking_t* envTracking;
	TxnWrap *currentWriteTxn;
	TxnTracked *writeTxn;
	pthread_mutex_t* writingLock;
	pthread_cond_t* writingCond;
	std::vector<AsyncWorker*> workers;

	txn_t* currentReadTxn;
	WriteWorker* writeWorker;
	bool readTxnRenewed;
	unsigned int jsFlags;
	char* keyBuffer;
	int pageSize;
	time_t lastReaderCheck;
	txn_t* getReadTxn();

	// Sets up exports for the Env constructor
	static void setupExports(Napi::Env env, Object exports);
	void closeEnv(bool hasLock = false);
	int openEnv(int flags, int jsFlags, const char* path, char* keyBuffer, Compression* compression, int maxDbs,
		int maxReaders, size_t mapSize, int pageSize, char* encryptionKey);

	/*
		Opens the database environment with the specified options. The options will be used to configure the environment before opening it.
		(Wrapper for `mdb_env_open`)

		Parameters:

		* Options object that contains possible configuration options.

		Possible options are:

		* maxDbs: the maximum number of named databases you can have in the environment (default is 1)
		* maxReaders: the maximum number of concurrent readers of the environment (default is 126)
		* mapSize: maximal size of the memory map (the full environment) in bytes (default is 10485760 bytes)
		* path: path to the database environment
	*/
	Napi::Value open(const CallbackInfo& info);
	Napi::Value close(const CallbackInfo& info);

	/*
		Performs a set of operations asynchronously, automatically wrapping it in its own transaction

		Parameters:

		* Callback to be executed after the sync is complete.
	*/
	Napi::Value startWriting(const CallbackInfo& info);
	static napi_value compress(napi_env env, napi_callback_info info);
	static napi_value write(napi_env env, napi_callback_info info);
	static napi_value onExit(napi_env env, napi_callback_info info);
	Napi::Value resetCurrentReadTxn(const CallbackInfo& info);
	static int32_t toSharedBuffer(splinterdb* env, uint32_t* keyBuffer, slice data);
};

const int TXN_ABORTABLE = 1;
const int TXN_SYNCHRONOUS_COMMIT = 2;
const int TXN_FROM_WORKER = 4;

/*
	`Txn`
	Represents a transaction running on a database environment.
	(Wrapper for `txn_t`)
*/
class TxnWrap : public ObjectWrap<TxnWrap> {
private:

	// Reference to the splinterdb of the wrapped txn_t
	splinterdb *env;

	// Environment wrapper of the current transaction
	EnvWrap *ew;
	// parent TW, if it is exists
	TxnWrap *parentTw;
	
	// Flags used with mdb_txn_begin
	unsigned int flags;

	friend class CursorWrap;
	friend class EnvWrap;

public:
	TxnWrap(const CallbackInfo& info);
	~TxnWrap();

	// The wrapped object
	txn_t *txn;

	// Remove the current TxnWrap from its EnvWrap
	void removeFromEnvWrap();
	int begin(EnvWrap *ew, unsigned int flags);

	/*
		Commits the transaction.
		(Wrapper for `mdb_txn_commit`)
	*/
	Napi::Value commit(const CallbackInfo& info);

	/*
		Aborts the transaction.
		(Wrapper for `mdb_txn_abort`)
	*/
	Napi::Value abort(const CallbackInfo& info);

	/*
		Aborts a read-only transaction but makes it renewable with `renew`.
		(Wrapper for `mdb_txn_reset`)
	*/
	void reset();
	/*
		Renews a read-only transaction after it has been reset.
		(Wrapper for `mdb_txn_renew`)
	*/
	Napi::Value renew(const CallbackInfo& info);
	static void setupExports(Napi::Env env, Object exports);
};

const int HAS_VERSIONS = 0x100;

class Compression : public ObjectWrap<Compression> {
public:
	char* dictionary; // dictionary to use to decompress
	char* compressDictionary; // separate dictionary to use to compress since the decompression dictionary can move around in the main thread
	unsigned int dictionarySize;
	char* decompressTarget;
	unsigned int decompressSize;
	unsigned int compressionThreshold;
	// compression acceleration (defaults to 1)
	int acceleration;
	static thread_local LZ4_stream_t* stream;
	void decompress(slice& data, bool &isValid, bool canAllocate);
	argtokey_callback_t compress(slice* value, argtokey_callback_t freeValue);
	int compressInstruction(EnvWrap* env, double* compressionAddress);
	Napi::Value ctor(const CallbackInfo& info);
	Napi::Value setBuffer(const CallbackInfo& info);
	Compression(const CallbackInfo& info);
	friend class EnvWrap;
	//NAN_METHOD(Compression::startCompressing);
	static void setupExports(Napi::Env env, Object exports);
};

/*
	`Cursor`
	Represents a cursor instance that is assigned to a transaction and a database instance
	(Wrapper for `splinterdb_iterator`)
*/
class CursorWrap : public ObjectWrap<CursorWrap> {

private:

	// Key/data pair where the cursor is at, and ending key
	slice key, data, endKey;
	// Free function for the current key
	argtokey_callback_t freeKey;

public:
	int iteratingOp;
	splinterdb_iterator *cursor;
	// Stores how key is represented
	LmdbKeyType keyType;
	int flags;
	txn_t *txn;

	// The wrapped object
	CursorWrap(splinterdb_iterator* cursor);
	CursorWrap(const CallbackInfo& info);
	~CursorWrap();

	// Sets up exports for the Cursor constructor
	static void setupExports(Napi::Env env, Object exports);

	/*
		Closes the cursor.
		(Wrapper for `mdb_cursor_close`)

		Parameters:

		* Transaction object
		* Database instance object
	*/
	Napi::Value close(const CallbackInfo& info);
	/*
		Deletes the key/data pair to which the cursor refers.
		(Wrapper for `mdb_cursor_del`)
	*/
	Napi::Value del(const CallbackInfo& info);

	int returnEntry(int lastRC, slice &key, slice &data);
	int32_t doPosition(uint32_t offset, uint32_t keySize, uint64_t endKeyAddress);
	//Value getStringByBinary(const CallbackInfo& info);
};

#endif // NODE_LMDB_H
