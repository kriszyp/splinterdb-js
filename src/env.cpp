extern "C" {
	#include "splinterdb/default_data_config.h"
	#include "splinterdb/transaction.h"
}
#include "splinterdb-js.h"
#include <atomic>
#ifndef _WIN32
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#endif
using namespace Napi;

#define IGNORE_NOTFOUND	(1)

env_tracking_t* DbWrap::envTracking = DbWrap::initTracking();
thread_local std::vector<DbWrap*>* DbWrap::openDbWraps = nullptr;
thread_local std::unordered_map<void*, buffer_info_t>* DbWrap::sharedBuffers = nullptr;
void* getSharedBuffers() {
	return (void*) DbWrap::sharedBuffers;
}

env_tracking_t* DbWrap::initTracking() {
	env_tracking_t* tracking = new env_tracking_t;
	tracking->dbsLock = new pthread_mutex_t;
	pthread_mutex_init(tracking->dbsLock, nullptr);
	tracking->getSharedBuffers = getSharedBuffers;
	return tracking;
}
static napi_ref testRef;
static napi_env testRefEnv;
void DbWrap::cleanupDbWraps(void* data) {
	if (openDbWraps)
		free(openDbWraps);
	else
		fprintf(stderr, "How do we end up cleanup env wraps that don't exist?\n");
	openDbWraps = nullptr;
}
DbWrap::DbWrap(const CallbackInfo& info) : ObjectWrap<DbWrap>(info) {

	this->currentWriteTxn = nullptr;
	this->currentReadTxn = nullptr;
	this->writeTxn = nullptr;
	this->writeWorker = nullptr;
	this->readTxnRenewed = false;
	this->writingLock = new pthread_mutex_t;
	this->writingCond = new pthread_cond_t;
	info.This().As<Object>().Set("address", Number::New(info.Env(), (size_t) this));
	pthread_mutex_init(this->writingLock, nullptr);
	pthread_cond_init(this->writingCond, nullptr);
}
splinterdb* foundEnv;

DbWrap::~DbWrap() {
	// Close if not closed already
	closeEnv();
	pthread_mutex_destroy(this->writingLock);
	pthread_cond_destroy(this->writingCond);
	
}

void DbWrap::cleanupStrayTxns() {
}
void DbWrap::consolidateTxns() {
	// sort read txns by txn id, and then abort newer ones that we can just reference older ones with.

}

void cleanup(void* data) {
	((DbWrap*) data)->closeEnv();
}

Napi::Value DbWrap::open(const CallbackInfo& info) {
	int rc;
	// Get the wrapper
	if (!this->db) {
		return throwError(info.Env(), "The environment is already closed.");
	}
	Object options = info[0].As<Object>();
	int flags = info[1].As<Number>();
	int jsFlags = info[2].As<Number>();

	Compression* compression = nullptr;
	Napi::Value compressionOption = options.Get("compression");
	if (compressionOption.IsObject()) {
		napi_unwrap(info.Env(), compressionOption, (void**)&compression);
		this->compression = compression;
	}
	void* keyBuffer;
	Napi::Value keyBytesValue = options.Get("keyBytes");
	if (!keyBytesValue.IsTypedArray())
		fprintf(stderr, "Invalid key buffer\n");
	size_t keyBufferLength;
	napi_get_buffer_info(info.Env(), keyBytesValue, &keyBuffer, &keyBufferLength);
	setFlagFromValue(&jsFlags, SEPARATE_FLUSHED, "separateFlushed", false, options);
	String path = options.Get("path").As<String>();
	std::string pathString = path.Utf8Value();
	// Parse the maxDbs option
	int maxDbs = 12;
	Napi::Value option = options.Get("maxDbs");
	if (option.IsNumber())
		maxDbs = option.As<Number>();

	size_t mapSize = 0;
	// Parse the mapSize option
	option = options.Get("mapSize");
	if (option.IsNumber())
		mapSize = option.As<Number>().Int64Value();
	int pageSize = 0;
	// Parse the mapSize option
	option = options.Get("pageSize");
	if (option.IsNumber())
		pageSize = option.As<Number>();
	int maxReaders = 126;
	// Parse the mapSize option
	option = options.Get("maxReaders");
	if (option.IsNumber())
		maxReaders = option.As<Number>();

	Napi::Value encryptionKey = options.Get("encryptionKey");
	std::string encryptKey;
	if (!encryptionKey.IsUndefined()) {
		encryptKey = encryptionKey.As<String>().Utf8Value();
		if (encryptKey.length() != 32) {
			return throwError(info.Env(), "Encryption key must be 32 bytes long");
		}
		#ifndef MDB_RPAGE_CACHE
		return throwError(info.Env(), "Encryption not supported with data format version 1");
		#endif
	}

	napiEnv = info.Env();
	rc = openDB(flags, jsFlags, (const char*)pathString.c_str(), (char*) keyBuffer, compression, maxDbs, maxReaders, mapSize, pageSize, encryptKey.empty() ? nullptr : (char*)encryptKey.c_str());
	//delete[] pathBytes;
	//if (rc < 0)
	//	return throwLmdbError(info.Env(), rc);
	napi_add_env_cleanup_hook(napiEnv, cleanup, this);
	return info.Env().Undefined();
}
int DbWrap::openDB(int flags, int jsFlags, const char* path, char* keyBuffer, Compression* compression, int maxDbs,
		int maxReaders, size_t mapSize, int pageSize, char* encryptionKey) {
	this->keyBuffer = keyBuffer;
	this->compression = compression;
	this->jsFlags = jsFlags;
	this->hasVersions = false;

// Initialize data configuration, using default key-comparison handling.
	data_config splinter_data_cfg;
	default_data_config_init(USER_MAX_KEY_SIZE, &splinter_data_cfg);

	// Basic configuration of a SplinterDB instance
	splinterdb_config splinterdb_cfg;
	memset(&splinterdb_cfg, 0, sizeof(splinterdb_cfg));
	splinterdb_cfg.filename	= path;
	splinterdb_cfg.disk_size  = 1024*1024*1024;
	splinterdb_cfg.cache_size = (64 * 1024 * 1024);
	splinterdb_cfg.data_cfg	= &splinter_data_cfg;

	this->db = NULL; // To a running SplinterDB instance

	int rc = transactional_splinterdb_create(&splinterdb_cfg, &db);
	return rc;
}
#ifdef _WIN32
// TODO: I think we should switch to DeleteFileW (but have to convert to UTF16)
#define unlink DeleteFileA
#else
#include <unistd.h>
#endif


NAPI_FUNCTION(DbWrap::onExit) {
	napi_value returnValue;
	RETURN_UNDEFINED;
}
NAPI_FUNCTION(getEnvsPointer) {
	napi_value returnValue;
	napi_create_double(env, (double) (size_t) DbWrap::envTracking, &returnValue);
	if (!DbWrap::sharedBuffers)
		DbWrap::sharedBuffers = new std::unordered_map<void*, buffer_info_t>;
	return returnValue;
}

NAPI_FUNCTION(setEnvsPointer) {
	// If another version of lmdb-js is running, switch to using its list of envs
	ARGS(2)
	GET_INT64_ARG(0);
	env_tracking_t* adoptedTracking = (env_tracking_t*) i64;
	// copy any existing ones over to the central one
	adoptedTracking->dbs.assign(DbWrap::envTracking->dbs.begin(), DbWrap::envTracking->dbs.end());
	DbWrap::envTracking = adoptedTracking;
	std::unordered_map<void*, buffer_info_t>* adoptedBuffers = (std::unordered_map<void*, buffer_info_t>*) adoptedTracking->getSharedBuffers();
	if (DbWrap::sharedBuffers && adoptedBuffers != DbWrap::sharedBuffers) {
		free(DbWrap::sharedBuffers);
	}
	DbWrap::sharedBuffers = adoptedBuffers;
	RETURN_UNDEFINED;
}

napi_finalize cleanupExternal = [](napi_env env, void *, void * size) {
	// Data belongs to LMDB, we shouldn't free it here
	int64_t result;
	napi_adjust_external_memory(env, (int64_t) size, &result);
	//fprintf(stderr, "adjust memory back up %i\n", result);
};

NAPI_FUNCTION(getSharedBuffer) {
	ARGS(2)
	uint32_t bufferId;
	GET_UINT32_ARG(bufferId, 0);
	GET_INT64_ARG(1);
	DbWrap* ew = (DbWrap*) i64;
	for (auto bufferRef = DbWrap::sharedBuffers->begin(); bufferRef != DbWrap::sharedBuffers->end(); bufferRef++) {
		if (bufferRef->second.id == bufferId) {
			void* start = bufferRef->first;
			buffer_info_t* buffer = &bufferRef->second;
			if (buffer->db == ew->db) {
				//fprintf(stderr, "found exiting buffer for %u\n", bufferId);
				napi_get_reference_value(env, buffer->ref, &returnValue);
				return returnValue;
			}
			if (buffer->db) {
				fprintf(stderr, "env changed");
				// if for some reason it is different env that didn't get cleaned up
				napi_value arrayBuffer;
				napi_get_reference_value(env, buffer->ref, &arrayBuffer);
				napi_detach_arraybuffer(env, arrayBuffer);
				napi_delete_reference(env, buffer->ref);
			}
			size_t end = buffer->end;
			buffer->db = ew->db;
			size_t size = end - (size_t) start;
			napi_create_external_arraybuffer(env, start, size, cleanupExternal, (void*) size, &returnValue);
			int64_t result;
			napi_create_reference(env, returnValue, 1, &buffer->ref);
			napi_adjust_external_memory(env, -(int64_t) size, &result);
			return returnValue;
		}
	}
	RETURN_UNDEFINED;
}
NAPI_FUNCTION(setTestRef) {
	ARGS(1)
	napi_create_reference(env, args[0], 1, &testRef);
	testRefEnv = env;
	RETURN_UNDEFINED
}

NAPI_FUNCTION(getTestRef) {
	napi_value returnValue;
	fprintf(stderr,"trying to get refernec\n");
	napi_get_reference_value(env, testRef, &returnValue);
	fprintf(stderr,"got refernec\n");
	return returnValue;
}

Napi::Value DbWrap::beginTxn(const CallbackInfo& info) {
	int flags = info[0].As<Number>();
	transactional_splinterdb_begin(db, &txn);
}
Napi::Value DbWrap::commitTxn(const CallbackInfo& info) {
//	TxnTracked *currentTxn = this->writeTxn;
	transactional_splinterdb_commit(db, &txn);
}
transaction* DbWrap::getReadTxn(int64_t tw_address) {
	transaction* txn;
	if (tw_address) // explicit txn
		txn = &((TxnWrap*)tw_address)->txn;
	/*else if (writeTxn && (txn = writeTxn->txn)) {
		return txn; // no need to renew write txn
	} */else // default to current read txn
		txn = currentReadTxn;
	txn = new transaction;
	int rc = transactional_splinterdb_begin(db, txn);
	if (rc) {
		if (!txn)
			fprintf(stderr, "No current read transaction available");
		if (rc != EINVAL)
			return nullptr; // if there was a real error, signal with nullptr and let error propagate with last_error
	}
	return txn;
}

int32_t DbWrap::doGetByBinary(uint32_t keySize, uint32_t ifNotTxnId, int64_t txnWrapAddress) {
	transaction* txn = getReadTxn(txnWrapAddress);
	slice key;
	slice data;
	key.length = keySize;
	key.data = (void*) keyBuffer;
	uint32_t* currentTxnId = (uint32_t*) (keyBuffer + 32);
	splinterdb_lookup_result result;
	transactional_splinterdb_lookup_result_init(db, &result, 0, NULL);

	int rc = transactional_splinterdb_lookup(db, txn, key, &result);
	rc = splinterdb_lookup_result_value(&result, &data);
	if (rc) {
		if (rc > 0)
			return -rc;
		return rc;
	}
	rc = getVersionAndUncompress(data, this);
	bool fits = true;
	if (rc) {
		fits = valToBinaryFast(data, this); // it fits in the global/compression-target buffer
	}
	if (fits || rc == 2 || data.length < SHARED_BUFFER_THRESHOLD) {// result = 2 if it was decompressed
		if (data.length < 0x80000000)
			return data.length;
		*((uint32_t*)keyBuffer) = data.length;
		return -30000;
	} else {
		return DbWrap::toSharedBuffer(db, (uint32_t*) keyBuffer, data);
	}
}

NAPI_FUNCTION(getByBinary) {
	ARGS(4)
	GET_INT64_ARG(0);
	DbWrap* dw = (DbWrap*) i64;
	uint32_t keySize;
	GET_UINT32_ARG(keySize, 1);
	uint32_t ifNotTxnId;
	GET_UINT32_ARG(ifNotTxnId, 2);
	int64_t txnAddress = 0;
	napi_status status = napi_get_value_int64(env, args[3], &txnAddress);
	RETURN_INT32(dw->doGetByBinary(keySize, ifNotTxnId, txnAddress));
}

thread_local int nextSharedId = 1;

void DbWrap::closeEnv(bool hasLock) {
	if (!db)
		return;
	db = nullptr;
}

Napi::Value DbWrap::close(const CallbackInfo& info) {
	if (!this->db) {
		return throwError(info.Env(), "The environment is already closed.");
	}
	this->closeEnv();
	return info.Env().Undefined();
}

void DbWrap::setupExports(Napi::Env env, Object exports) {
	// DbWrap: Prepare constructor template
	Function EnvClass = ObjectWrap<DbWrap>::DefineClass(env, "Env", {
		DbWrap::InstanceMethod("open", &DbWrap::open),
		DbWrap::InstanceMethod("close", &DbWrap::close),
		DbWrap::InstanceMethod("beginTxn", &DbWrap::beginTxn),
		DbWrap::InstanceMethod("commitTxn", &DbWrap::commitTxn),
		DbWrap::InstanceMethod("startWriting", &DbWrap::startWriting),
	});
	//envTpl->InstanceTemplate()->SetInternalFieldCount(1);
	//EXPORT_NAPI_FUNCTION("compress", compress);
	EXPORT_NAPI_FUNCTION("write", write);
	EXPORT_NAPI_FUNCTION("getByBinary", getByBinary);
	exports.Set("Env", EnvClass);
}

// This file contains code from the node-lmdb project
// Copyright (c) 2013-2017 Timur Kristóf
// Copyright (c) 2021 Kristopher Tate
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

