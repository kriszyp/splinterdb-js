#include "lmdb-js.h"
#include <atomic>
#ifndef _WIN32
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#endif
using namespace Napi;

#define IGNORE_NOTFOUND	(1)

env_tracking_t* EnvWrap::envTracking = EnvWrap::initTracking();
thread_local std::vector<EnvWrap*>* EnvWrap::openEnvWraps = nullptr;
thread_local std::unordered_map<void*, buffer_info_t>* EnvWrap::sharedBuffers = nullptr;
void* getSharedBuffers() {
	return (void*) EnvWrap::sharedBuffers;
}

env_tracking_t* EnvWrap::initTracking() {
	env_tracking_t* tracking = new env_tracking_t;
	tracking->envsLock = new pthread_mutex_t;
	pthread_mutex_init(tracking->envsLock, nullptr);
	tracking->getSharedBuffers = getSharedBuffers;
	return tracking;
}
static napi_ref testRef;
static napi_env testRefEnv;
void EnvWrap::cleanupEnvWraps(void* data) {
	if (openEnvWraps)
		free(openEnvWraps);
	else
		fprintf(stderr, "How do we end up cleanup env wraps that don't exist?\n");
	openEnvWraps = nullptr;
}
EnvWrap::EnvWrap(const CallbackInfo& info) : ObjectWrap<EnvWrap>(info) {
	int rc;
	rc = mdb_env_create(&(this->env));

	if (rc != 0) {
		mdb_env_close(this->env);
		throwLmdbError(info.Env(), rc);
		return;
	}

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
MDB_env* foundEnv;
const int EXISTING_ENV_FOUND = 10;
int checkExistingEnvs(mdb_filehandle_t fd, MDB_env* env) {
	uint64_t inode, dev;
	#ifdef _WIN32
	BY_HANDLE_FILE_INFORMATION fileInformation;
	if (GetFileInformationByHandle(fd, &fileInformation)) {
		dev = fileInformation.dwVolumeSerialNumber;
		inode = ((uint64_t) fileInformation.nFileIndexHigh << 32) | fileInformation.nFileIndexLow;
	} else
		return MDB_NOTFOUND;
	#else
	struct stat sb;
	if (fstat(fd, &sb) == 0) {
		dev = sb.st_dev;
		inode = sb.st_ino;
	} else
		return MDB_NOTFOUND;
	#endif
	for (auto envRef = EnvWrap::envTracking->envs.begin(); envRef != EnvWrap::envTracking->envs.end();) {
		if (envRef->dev == dev && envRef->inode == inode) {
			envRef->count++;
			foundEnv = envRef->env;
			return EXISTING_ENV_FOUND;
		}
		++envRef;
	}
	SharedEnv envRef;
	envRef.dev = dev;
	envRef.inode = inode;
	envRef.env = env;
	envRef.count = 1;
	EnvWrap::envTracking->envs.push_back(envRef);
	return 0;
}

EnvWrap::~EnvWrap() {
	// Close if not closed already
	closeEnv();
	pthread_mutex_destroy(this->writingLock);
	pthread_cond_destroy(this->writingCond);
	
}

void EnvWrap::cleanupStrayTxns() {
	if (this->currentWriteTxn) {
		mdb_txn_abort(this->currentWriteTxn->txn);
		this->currentWriteTxn->removeFromEnvWrap();
	}
/*	while (this->workers.size()) { // enable this if we do need to do worker cleanup
		AsyncWorker *worker = *this->workers.begin();
		fprintf(stderr, "Deleting running worker\n");
		delete worker;
	}*/
	while (this->readTxns.size()) {
		TxnWrap *tw = *this->readTxns.begin();
		mdb_txn_abort(tw->txn);
		tw->removeFromEnvWrap();
	}
}
void EnvWrap::consolidateTxns() {
	// sort read txns by txn id, and then abort newer ones that we can just reference older ones with.

}

class SyncWorker : public AsyncWorker {
  public:
	SyncWorker(EnvWrap* env, const Function& callback)
	 : AsyncWorker(callback), env(env) {
		//env->workers.push_back(this);
	 }
	/*~SyncWorker() {
		for (auto workerRef = env->workers.begin(); workerRef != env->workers.end(); ) {
			if (this == *workerRef) {
				env->workers.erase(workerRef);
			}
		}
	}*/
	void OnOK() {
		napi_value result; // we use direct napi call here because node-addon-api interface with throw a fatal error if a worker thread is terminating
		napi_call_function(Env(), Env().Undefined(), Callback().Value(), 0, {}, &result);
	}

	void Execute() {
		#ifdef _WIN32
		int rc = mdb_env_sync(env->env, 1);
		#else
		int retries = 0;
		retry:
		int rc = mdb_env_sync(env->env, 1);
		if (rc == MDB_LOCK_FAILURE) {
			if (retries++ < 4) {
				sleep(1);
				goto retry;
			}
		}
		#endif
		if (rc != 0) {
			SetError(mdb_strerror(rc));
		}
	}

  private:
	EnvWrap* env;
};

class CopyWorker : public AsyncWorker {
  public:
	CopyWorker(MDB_env* env, std::string inPath, int flags, const Function& callback)
	 : AsyncWorker(callback), env(env), path(inPath), flags(flags) {
	 }
	~CopyWorker() {
		//free(path);
	}

	void Execute() {
		int rc = mdb_env_copy2(env, path.c_str(), flags);
		if (rc != 0) {
			fprintf(stderr, "Error on copy code: %u\n", rc);
			SetError("Error on copy");
		}
	}

  private:
	MDB_env* env;
	std::string path;
	int flags;
};

class ReadWorker : public AsyncWorker {
  public:
	ReadWorker(uint32_t* start, const Function& callback)
	  : AsyncWorker(callback), start(start) {}

	void Execute() {
		uint32_t instruction;
		uint32_t* gets = start;
		while((instruction = std::atomic_exchange((std::atomic<uint32_t>*)(gets++), (uint32_t)0xf0000000))) {

			slice key;
			key.mv_size = instruction & 0xffff;
			MDB_dbi dbi = (MDB_dbi) *(gets++);
			slice data;
			MDB_txn* txn = (MDB_txn*) (size_t) *((double*)gets);
			gets += 2;
			
			unsigned int flags;
			mdb_dbi_flags(txn, dbi, &flags);
			bool dupSort = flags & MDB_DUPSORT;
			int effected = 0;
			MDB_cursor *cursor;
			int rc = mdb_cursor_open(txn, dbi, &cursor);
			if (rc)
				return SetError(mdb_strerror(rc));

			key.mv_data = (void*) gets;
			gets += (key.mv_size + 12) >> 2;
			rc = mdb_cursor_get(cursor, &key, &data, MDB_SET_KEY);
			MDB_env* env = mdb_txn_env(txn);

			while (!rc) {
				// access one byte from each of the pages to ensure they are in the OS cache,
				// potentially triggering the hard page fault in this thread
				int pages = (data.mv_size + 0xfff) >> 12;
				// TODO: Adjust this for the page headers, I believe that makes the first page slightly less 4KB.
				for (int i = 0; i < pages; i++) {
					effected += *(((uint8_t*)data.mv_data) + (i << 12));
				}
				if (dupSort) // in dupsort databases, access the rest of the values
					rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT_DUP);
				else
					rc = 1; // done
			
			}
			mdb_cursor_close(cursor);
		}
	}

	void OnOK() {
		// TODO: For each entry, find the shared buffer
		uint32_t* gets = start;
		// EnvWrap::toSharedBuffer();
		Callback().Call({Env().Null()});
	}

  private:
	uint32_t* start;
};

NAPI_FUNCTION(readNapi) {
	ARGS(2)
	GET_INT64_ARG(0);
	uint32_t* instructionAddress = (uint32_t*) i64;
	ReadWorker* worker = new ReadWorker(instructionAddress, Function(env, args[1]));
	worker->Queue();
	RETURN_UNDEFINED;
}




MDB_txn* EnvWrap::getReadTxn() {
	MDB_txn* txn = writeTxn ? writeTxn->txn : nullptr;
	if (txn)
		return txn;
	txn = currentReadTxn;
	if (readTxnRenewed)
		return txn;
	if (txn) {
		int rc = mdb_txn_renew(txn);
		if (rc) {
			if (rc != EINVAL)
				return nullptr; // if there was a real error, signal with nullptr and let error propagate with last_error
		}
	} else {
		fprintf(stderr, "No current read transaction available");
		return nullptr;
	}
	readTxnRenewed = true;
	return txn;
}

#ifdef MDB_RPAGE_CACHE
static int encfunc(const slice* src, slice* dst, const slice* key, int encdec)
{
	chacha8(src->mv_data, src->mv_size, (uint8_t*) key[0].mv_data, (uint8_t*) key[1].mv_data, (char*)dst->mv_data);
	return 0;
}
#endif

void cleanup(void* data) {
	((EnvWrap*) data)->closeEnv();
}

Napi::Value EnvWrap::open(const CallbackInfo& info) {
	int rc;
	// Get the wrapper
	if (!this->env) {
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
	napi_get_typedarray_info(info.Env(), keyBytesValue, nullptr, &keyBufferLength, &keyBuffer, nullptr, nullptr);
	setFlagFromValue(&jsFlags, SEPARATE_FLUSHED, "separateFlushed", false, options);
	String path = options.Get("path").As<String>();
	std::string pathString = path.Utf8Value();
	// Parse the maxDbs option
	int maxDbs = 12;
	Napi::Value option = options.Get("maxDbs");
	if (option.IsNumber())
		maxDbs = option.As<Number>();

	mdb_size_t mapSize = 0;
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
	rc = openEnv(flags, jsFlags, (const char*)pathString.c_str(), (char*) keyBuffer, compression, maxDbs, maxReaders, mapSize, pageSize, encryptKey.empty() ? nullptr : (char*)encryptKey.c_str());
	//delete[] pathBytes;
	if (rc < 0)
		return throwLmdbError(info.Env(), rc);
	napi_add_env_cleanup_hook(napiEnv, cleanup, this);
	return info.Env().Undefined();
}
int EnvWrap::openEnv(int flags, int jsFlags, const char* path, char* keyBuffer, Compression* compression, int maxDbs,
		int maxReaders, mdb_size_t mapSize, int pageSize, char* encryptionKey) {
	this->keyBuffer = keyBuffer;
	this->compression = compression;
	this->jsFlags = jsFlags;


// Initialize data configuration, using default key-comparison handling.
	data_config splinter_data_cfg;
	default_data_config_init(USER_MAX_KEY_SIZE, &splinter_data_cfg);

	// Basic configuration of a SplinterDB instance
	splinterdb_config splinterdb_cfg;
	memset(&splinterdb_cfg, 0, sizeof(splinterdb_cfg));
	splinterdb_cfg.filename	= path;
	splinterdb_cfg.disk_size  = mapSize;
	splinterdb_cfg.cache_size = (10 * 1024 * 1024);
	splinterdb_cfg.data_cfg	= &splinter_data_cfg;

	splinterdb *spl_handle = NULL; // To a running SplinterDB instance

	int rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
	return rc;
}
Napi::Value EnvWrap::getMaxKeySize(const CallbackInfo& info) {
	return Number::New(info.Env(), mdb_env_get_maxkeysize(this->env));
}

NAPI_FUNCTION(getEnvFlags) {
	ARGS(1)
	GET_INT64_ARG(0);
	EnvWrap* ew = (EnvWrap*) i64;
	unsigned int envFlags;
	mdb_env_get_flags(ew->env, &envFlags);
	RETURN_UINT32(envFlags);
}

NAPI_FUNCTION(setJSFlags) {
	ARGS(2)
	GET_INT64_ARG(0);
	EnvWrap* ew = (EnvWrap*) i64;
	int64_t jsFlags;
	napi_get_value_int64(env, args[1], &jsFlags);
	ew->jsFlags = jsFlags;
	RETURN_UNDEFINED;
}

#ifdef _WIN32
// TODO: I think we should switch to DeleteFileW (but have to convert to UTF16)
#define unlink DeleteFileA
#else
#include <unistd.h>
#endif


NAPI_FUNCTION(EnvWrap::onExit) {
	// close all the environments
	if (openEnvWraps) {
		for (auto envWrap : *openEnvWraps)
			envWrap->closeEnv();
	}
	napi_value returnValue;
	RETURN_UNDEFINED;
}
NAPI_FUNCTION(getEnvsPointer) {
	napi_value returnValue;
	napi_create_double(env, (double) (size_t) EnvWrap::envTracking, &returnValue);
	if (!EnvWrap::sharedBuffers)
		EnvWrap::sharedBuffers = new std::unordered_map<void*, buffer_info_t>;
	return returnValue;
}

NAPI_FUNCTION(setEnvsPointer) {
	// If another version of lmdb-js is running, switch to using its list of envs
	ARGS(2)
	GET_INT64_ARG(0);
	env_tracking_t* adoptedTracking = (env_tracking_t*) i64;
	// copy any existing ones over to the central one
	adoptedTracking->envs.assign(EnvWrap::envTracking->envs.begin(), EnvWrap::envTracking->envs.end());
	EnvWrap::envTracking = adoptedTracking;
	std::unordered_map<void*, buffer_info_t>* adoptedBuffers = (std::unordered_map<void*, buffer_info_t>*) adoptedTracking->getSharedBuffers();
	if (EnvWrap::sharedBuffers && adoptedBuffers != EnvWrap::sharedBuffers) {
		free(EnvWrap::sharedBuffers);
	}
	EnvWrap::sharedBuffers = adoptedBuffers;
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
	EnvWrap* ew = (EnvWrap*) i64;
	for (auto bufferRef = EnvWrap::sharedBuffers->begin(); bufferRef != EnvWrap::sharedBuffers->end(); bufferRef++) {
		if (bufferRef->second.id == bufferId) {
			void* start = bufferRef->first;
			buffer_info_t* buffer = &bufferRef->second;
			if (buffer->env == ew->env) {
				//fprintf(stderr, "found exiting buffer for %u\n", bufferId);
				napi_get_reference_value(env, buffer->ref, &returnValue);
				return returnValue;
			}
			if (buffer->env) {
				fprintf(stderr, "env changed");
				// if for some reason it is different env that didn't get cleaned up
				napi_value arrayBuffer;
				napi_get_reference_value(env, buffer->ref, &arrayBuffer);
				napi_detach_arraybuffer(env, arrayBuffer);
				napi_delete_reference(env, buffer->ref);
			}
			size_t end = buffer->end;
			buffer->env = ew->env;
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

NAPI_FUNCTION(directWrite) {
	ARGS(4)
	GET_INT64_ARG(0);
	EnvWrap* ew = (EnvWrap*) i64;
	napi_get_value_int64(env, args[1], &i64);
	char* target = (char*) i64;
	napi_get_value_int64(env, args[2], &i64);
	void* source = (void*) i64;
	uint32_t length;
	GET_UINT32_ARG(length, 3);
	mdb_filehandle_t fd;
	mdb_env_get_fd(ew->env, &fd);
	MDB_envinfo stat;
	mdb_env_info(ew->env, &stat);
	int64_t offset = target - (char*) stat.me_mapaddr;
	if (offset > 0 && offset < (int64_t) stat.me_mapsize) {
		#ifdef _WIN32
		OVERLAPPED ov;
		ov.Offset = offset;
		ov.OffsetHigh = 0;
		WriteFile(fd, source, length, nullptr, &ov);
		#else
		pwrite(fd, source, length, offset);
		#endif
	}
	RETURN_UNDEFINED;
}
thread_local int nextSharedId = 1;

int32_t EnvWrap::toSharedBuffer(MDB_env* env, uint32_t* keyBuffer,  slice data) {
	unsigned int flags;
	mdb_env_get_flags(env, (unsigned int*) &flags);
	#ifdef MDB_RPAGE_CACHE
	if (flags & MDB_REMAP_CHUNKS) {
		*((uint32_t*)keyBuffer) = data.mv_size;
		*((uint32_t*) (keyBuffer + 4)) = 0;
		return -30000;
	}
	#endif
	MDB_envinfo stat;
	mdb_env_info(env, &stat);
	size_t mapAddress = (size_t) (char*) stat.me_mapaddr;
	size_t dataAddress = (size_t) (char*) data.mv_data;
	int64_t mapOffset = dataAddress - mapAddress;
	size_t bufferPosition = (mapOffset + (mapOffset >> 4)) >> 32;
	size_t bufferStart = bufferPosition << 32;
	bufferStart += mapAddress - (bufferStart >> 4);
	//fprintf(stderr, "mapAddress %p bufferStart %p", mapAddress, bufferStart);
	auto bufferSearch = sharedBuffers->find((void*)bufferStart);
	size_t offset = dataAddress - bufferStart;
	buffer_info_t bufferInfo;
	uint64_t end;
	if (bufferSearch == sharedBuffers->end()) {
		end = bufferStart + 0xffffffffll;
		if (end > mapAddress + stat.me_mapsize)
			end = mapAddress + stat.me_mapsize;
	} else {
		bufferInfo = bufferSearch->second;
		end = bufferInfo.end;
	}
	if (end < dataAddress + data.mv_size || mapOffset < 0) {
		bufferSearch = sharedBuffers->find((void*)(bufferStart = dataAddress));
		offset = 0;
		if (bufferSearch != sharedBuffers->end())
			bufferInfo = bufferSearch->second;
	}
	if (bufferSearch == sharedBuffers->end()) {
		end = bufferStart + 0xffffffffll;
		if (end > mapAddress + stat.me_mapsize)
			end = mapAddress + stat.me_mapsize;
		bufferInfo.end = end;
		bufferInfo.env = nullptr;
		bufferInfo.id = nextSharedId++;
		sharedBuffers->emplace((void*)bufferStart, bufferInfo);
	}
	*keyBuffer = data.mv_size;
	*(keyBuffer + 1) = bufferInfo.id;
	*(keyBuffer + 2) = offset;
	return -30001;
}


void EnvWrap::closeEnv(bool hasLock) {
	if (!env)
		return;
	if (openEnvWraps) {
		for (auto ewRef = openEnvWraps->begin(); ewRef != openEnvWraps->end(); ) {
			if (*ewRef == this) {
				openEnvWraps->erase(ewRef);
				break;
			}
			++ewRef;
		}
	}
	napi_remove_env_cleanup_hook(napiEnv, cleanup, this);
	cleanupStrayTxns();
	if (!hasLock)
		pthread_mutex_lock(envTracking->envsLock);
	for (auto envPath = envTracking->envs.begin(); envPath != envTracking->envs.end(); ) {
		if (envPath->env == env) {
			envPath->count--;
			if (envPath->count <= 0) {
				// last thread using it, we can really close it now
				unsigned int envFlags; // This is primarily useful for detecting termination of threads and sync'ing on their termination
				mdb_env_get_flags(env, &envFlags);
				#ifdef MDB_OVERLAPPINGSYNC
				if (envFlags & MDB_OVERLAPPINGSYNC) {
					mdb_env_sync(env, 1);
				}
				#endif
				char* path;
				mdb_env_get_path(env, (const char**)&path);
				path = strdup(path);
				mdb_env_close(env);
				for (auto bufferRef = EnvWrap::sharedBuffers->begin(); bufferRef != EnvWrap::sharedBuffers->end();) {
					if (bufferRef->second.env == env) {
						napi_value arrayBuffer;
						napi_get_reference_value(napiEnv, bufferRef->second.ref, &arrayBuffer);
						napi_detach_arraybuffer(napiEnv, arrayBuffer);
						napi_delete_reference(napiEnv, bufferRef->second.ref);
						bufferRef = EnvWrap::sharedBuffers->erase(bufferRef);
					} else
						bufferRef++;
				}
				if (jsFlags & DELETE_ON_CLOSE) {
					unlink(path);
					//unlink(strcat(envPath->path, "-lock"));
				}
				envTracking->envs.erase(envPath);
			}
			break;
		}
		++envPath;
	}
	pthread_mutex_unlock(envTracking->envsLock);
	env = nullptr;
}

Napi::Value EnvWrap::close(const CallbackInfo& info) {
	if (!this->env) {
		return throwError(info.Env(), "The environment is already closed.");
	}
	this->closeEnv();
	return info.Env().Undefined();
}

Napi::Value EnvWrap::stat(const CallbackInfo& info) {
	if (!this->env) {
		return throwError(info.Env(), "The environment is already closed.");
	}
	int rc;
	MDB_stat stat;

	rc = mdb_env_stat(this->env, &stat);
	if (rc != 0) {
		return throwLmdbError(info.Env(), rc);
	}
	Object stats = Object::New(info.Env());
	stats.Set("pageSize", Number::New(info.Env(), stat.ms_psize));
	stats.Set("treeDepth", Number::New(info.Env(), stat.ms_depth));
	stats.Set("treeBranchPageCount", Number::New(info.Env(), stat.ms_branch_pages));
	stats.Set("treeLeafPageCount", Number::New(info.Env(), stat.ms_leaf_pages));
	stats.Set("entryCount", Number::New(info.Env(), stat.ms_entries));
	stats.Set("overflowPages", Number::New(info.Env(), stat.ms_overflow_pages));
	return stats;
}

Napi::Value EnvWrap::freeStat(const CallbackInfo& info) {
	if (!this->env) {
		return throwError(info.Env(),"The environment is already closed.");
	}
	int rc;
	MDB_stat stat;
	MDB_txn *txn = getReadTxn();
	rc = mdb_stat(txn, 0, &stat);
	if (rc != 0) {
		return throwLmdbError(info.Env(), rc);
	}
	Object stats = Object::New(info.Env());
	stats.Set("pageSize", Number::New(info.Env(), stat.ms_psize));
	stats.Set("treeDepth", Number::New(info.Env(), stat.ms_depth));
	stats.Set("treeBranchPageCount", Number::New(info.Env(), stat.ms_branch_pages));
	stats.Set("treeLeafPageCount", Number::New(info.Env(), stat.ms_leaf_pages));
	stats.Set("entryCount", Number::New(info.Env(), stat.ms_entries));
	stats.Set("overflowPages", Number::New(info.Env(), stat.ms_overflow_pages));
	return stats;
}

Napi::Value EnvWrap::info(const CallbackInfo& info) {
	if (!this->env) {
		return throwError(info.Env(),"The environment is already closed.");
	}
	int rc;
	MDB_envinfo envinfo;

	rc = mdb_env_info(this->env, &envinfo);
	if (rc != 0) {
		return throwLmdbError(info.Env(), rc);
	}
	Object stats = Object::New(info.Env());
	stats.Set("mapSize", Number::New(info.Env(), envinfo.me_mapsize));
	stats.Set("lastPageNumber", Number::New(info.Env(), envinfo.me_last_pgno));
	stats.Set("lastTxnId", Number::New(info.Env(), envinfo.me_last_txnid));
	stats.Set("maxReaders", Number::New(info.Env(), envinfo.me_maxreaders));
	stats.Set("numReaders", Number::New(info.Env(), envinfo.me_numreaders));
	return stats;
}

Napi::Value EnvWrap::readerCheck(const CallbackInfo& info) {
	if (!this->env) {
		return throwError(info.Env(), "The environment is already closed.");
	}

	int rc, dead;
	rc = mdb_reader_check(this->env, &dead);
	if (rc != 0) {
		return throwLmdbError(info.Env(), rc);
	}
	return Number::New(info.Env(), dead);
}

Array readerStrings;
MDB_msg_func* printReaders = ([](const char* message, void* env) -> int {
	readerStrings.Set(readerStrings.Length(), String::New(*(Env*)env, message));
	return 0;
});

Napi::Value EnvWrap::readerList(const CallbackInfo& info) {
	if (!this->env) {
		return throwError(info.Env(), "The environment is already closed.");
	}
	readerStrings = Array::New(info.Env());
	int rc;
	Napi::Env env = info.Env();
	rc = mdb_reader_list(this->env, printReaders, &env);
	if (rc != 0) {
		return throwLmdbError(info.Env(), rc);
	}
	return readerStrings;
}


Napi::Value EnvWrap::copy(const CallbackInfo& info) {
	if (!this->env) {
		return throwError(info.Env(), "The environment is already closed.");
	}

	// Check that the correct number/type of arguments was given.
	if (!info[0].IsString()) {
		return throwError(info.Env(), "Call env.copy(path, compact?, callback) with a file path.");
	}
	if (!info[info.Length() - 1].IsFunction()) {
		return throwError(info.Env(), "Call env.copy(path, compact?, callback) with a file path.");
	}

	int flags = 0;
	if (info.Length() > 1 && info[1].IsBoolean() && info[1].ToBoolean()) {
		flags = MDB_CP_COMPACT;
	}

	CopyWorker* worker = new CopyWorker(
		this->env, info[0].As<String>().Utf8Value(), flags, info[info.Length()	> 2 ? 2 : 1].As<Function>()
	);
	worker->Queue();
	return info.Env().Undefined();
}

Napi::Value EnvWrap::beginTxn(const CallbackInfo& info) {
	int flags = info[0].As<Number>();
	if (!(flags & MDB_RDONLY)) {
		MDB_env *env = this->env;
		unsigned int envFlags;
		mdb_env_get_flags(env, &envFlags);
		MDB_txn *txn;

		if (this->writeTxn)
			txn = this->writeTxn->txn;
		else if (this->writeWorker) {
			// try to acquire the txn from the current batch
			txn = this->writeWorker->AcquireTxn(&flags);
		} else {
			pthread_mutex_lock(this->writingLock);
			txn = nullptr;
		}

		if (txn) {
			if (flags & TXN_ABORTABLE) {
				if (envFlags & MDB_WRITEMAP)
					flags &= ~TXN_ABORTABLE;
				else {
					// child txn
					mdb_txn_begin(env, txn, flags & 0xf0000, &txn);
					TxnTracked* childTxn = new TxnTracked(txn, flags);
					childTxn->parent = this->writeTxn;
					this->writeTxn = childTxn;
					return info.Env().Undefined();
				}
			}
		} else {
			mdb_txn_begin(env, nullptr, flags & 0xf0000, &txn);
			flags |= TXN_ABORTABLE;
		}
		this->writeTxn = new TxnTracked(txn, flags);
		return info.Env().Undefined();
	}

	if (info.Length() > 1) {
		fprintf(stderr, "Invalid number of arguments");
	} else {
		fprintf(stderr, "Invalid number of arguments");
	}
	return info.Env().Undefined();
}
Napi::Value EnvWrap::commitTxn(const CallbackInfo& info) {
	TxnTracked *currentTxn = this->writeTxn;
	//fprintf(stderr, "commitTxn %p\n", currentTxn);
	int rc = 0;
	if (currentTxn->flags & TXN_ABORTABLE) {
		//fprintf(stderr, "txn_commit\n");
		rc = mdb_txn_commit(currentTxn->txn);
	}
	this->writeTxn = currentTxn->parent;
	if (!this->writeTxn) {
		//fprintf(stderr, "unlock txn\n");
		if (this->writeWorker)
			this->writeWorker->UnlockTxn();
		else
			pthread_mutex_unlock(this->writingLock);
	}
	delete currentTxn;
	if (rc)
		throwLmdbError(info.Env(), rc);
	return info.Env().Undefined();
}
Napi::Value EnvWrap::abortTxn(const CallbackInfo& info) {
	TxnTracked *currentTxn = this->writeTxn;
	if (currentTxn->flags & TXN_ABORTABLE) {
		mdb_txn_abort(currentTxn->txn);
	} else {
		throwError(info.Env(), "Can not abort this transaction");
	}
	this->writeTxn = currentTxn->parent;
	if (!this->writeTxn) {
		if (this->writeWorker)
			this->writeWorker->UnlockTxn();
		else
			pthread_mutex_unlock(this->writingLock);
	}
	delete currentTxn;
	return info.Env().Undefined();
}
/*Napi::Value EnvWrap::openDbi(const CallbackInfo& info) {


	const unsigned argc = 5;
	Local<Value> argv[argc] = { info.This(), info[0], info[1], info[2], info[3] };
	Nan::MaybeLocal<Object> maybeInstance = Nan::NewInstance(Nan::New(*dbiCtor), argc, argv);

	// Check if database could be opened
	if ((maybeInstance.IsEmpty())) {
		// The maybeInstance is empty because the dbiCtor called throwError.
		// No need to call that here again, the user will get the error thrown there.
		return;
	}

	Local<Object> instance = maybeInstance.ToLocalChecked();
	DbiWrap *dw = Nan::ObjectWrap::Unwrap<DbiWrap>(instance);
	if (dw->dbi == (MDB_dbi) 0xffffffff)
		info.GetReturnValue().Set(Nan::Undefined());
	else
		info.GetReturnValue().Set(instance);
}*/

Napi::Value EnvWrap::sync(const CallbackInfo& info) {

	if (!this->env) {
		return throwError(info.Env(), "The environment is already closed.");
	}
	if (info.Length() > 0) {
		SyncWorker* worker = new SyncWorker(this, info[0].As<Function>());
		worker->Queue();
	} else {
		int rc = mdb_env_sync(this->env, 1);
		if (rc != 0) {
			return throwLmdbError(info.Env(), rc);
		}
	}
	return info.Env().Undefined();
}

Napi::Value EnvWrap::resetCurrentReadTxn(const CallbackInfo& info) {
	mdb_txn_reset(this->currentReadTxn);
	this->readTxnRenewed = false;
	return info.Env().Undefined();
}
int32_t writeFFI(double ewPointer, uint64_t instructionAddress) {
	EnvWrap* ew = (EnvWrap*) (size_t) ewPointer;
	int rc;
	if (instructionAddress)
		rc = WriteWorker::DoWrites(ew->writeTxn->txn, ew, (uint32_t*)instructionAddress, nullptr);
	else {
		pthread_cond_signal(ew->writingCond);
		rc = 0;
	}
	return rc;
}


void EnvWrap::setupExports(Napi::Env env, Object exports) {
	// EnvWrap: Prepare constructor template
	Function EnvClass = ObjectWrap<EnvWrap>::DefineClass(env, "Env", {
		EnvWrap::InstanceMethod("open", &EnvWrap::open),
		EnvWrap::InstanceMethod("getMaxKeySize", &EnvWrap::getMaxKeySize),
		EnvWrap::InstanceMethod("close", &EnvWrap::close),
		EnvWrap::InstanceMethod("beginTxn", &EnvWrap::beginTxn),
		EnvWrap::InstanceMethod("commitTxn", &EnvWrap::commitTxn),
		EnvWrap::InstanceMethod("abortTxn", &EnvWrap::abortTxn),
		EnvWrap::InstanceMethod("sync", &EnvWrap::sync),
		EnvWrap::InstanceMethod("startWriting", &EnvWrap::startWriting),
		EnvWrap::InstanceMethod("stat", &EnvWrap::stat),
		EnvWrap::InstanceMethod("freeStat", &EnvWrap::freeStat),
		EnvWrap::InstanceMethod("info", &EnvWrap::info),
		EnvWrap::InstanceMethod("readerCheck", &EnvWrap::readerCheck),
		EnvWrap::InstanceMethod("readerList", &EnvWrap::readerList),
		EnvWrap::InstanceMethod("copy", &EnvWrap::copy),
		//EnvWrap::InstanceMethod("detachBuffer", &EnvWrap::detachBuffer),
		EnvWrap::InstanceMethod("resetCurrentReadTxn", &EnvWrap::resetCurrentReadTxn),
	});
	EXPORT_NAPI_FUNCTION("compress", compress);
	EXPORT_NAPI_FUNCTION("write", write);
	EXPORT_NAPI_FUNCTION("onExit", onExit);
	EXPORT_NAPI_FUNCTION("getEnvsPointer", getEnvsPointer);
	EXPORT_NAPI_FUNCTION("setEnvsPointer", setEnvsPointer);
	EXPORT_NAPI_FUNCTION("getEnvFlags", getEnvFlags);
	EXPORT_NAPI_FUNCTION("setJSFlags", setJSFlags);
	EXPORT_NAPI_FUNCTION("getSharedBuffer", getSharedBuffer);
	EXPORT_NAPI_FUNCTION("setTestRef", setTestRef);
	EXPORT_NAPI_FUNCTION("getTestRef", getTestRef);
	EXPORT_FUNCTION_ADDRESS("writePtr", writeFFI);
	//envTpl->InstanceTemplate()->SetInternalFieldCount(1);
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

