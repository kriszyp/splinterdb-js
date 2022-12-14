/* write instructions

0-3 flags
4-7 dbi
8-11 key-size
12 ... key followed by at least 2 32-bit zeros
4 value-size
8 bytes: value pointer (or value itself)
8 compressor pointer?
8 bytes (optional): conditional version
8 bytes (optional): version
inline value?
*/
#include "splinterdb-js.h"
#include <atomic>
#include <ctime>
#ifndef _WIN32
#include <unistd.h>
#endif

// flags:
const uint32_t NO_INSTRUCTION_YET = 0;
const int PUT = 15;
const int DEL = 13;
const int DEL_VALUE = 14;
const int START_CONDITION_BLOCK = 4;
//const int START_CONDITION_VALUE_BLOCK = 6;
const int START_BLOCK = 1;
const int BLOCK_END = 2;
const int POINTER_NEXT = 3;
const int USER_CALLBACK = 8;
const int USER_CALLBACK_STRICT_ORDER = 0x100000;
const int DROP_DB = 12;
const int HAS_KEY = 4;
const int HAS_VALUE = 2;
const int CONDITIONAL = 8;
const int CONDITIONAL_VERSION = 0x100;
const int CONDITIONAL_VERSION_LESS_THAN = 0x800;
const int CONDITIONAL_ALLOW_NOTFOUND = 0x1000;
const int SET_VERSION = 0x200;
//const int HAS_INLINE_VALUE = 0x400;
const int COMPRESSIBLE = 0x100000;
const int DELETE_DATABASE = 0x400;
const int TXN_HAD_ERROR = 0x40000000;
const int TXN_DELIMITER = 0x8000000;
const int TXN_COMMITTED = 0x10000000;
//const int TXN_FLUSHED = 0x20000000;
const int WAITING_OPERATION = 0x2000000;
const int IF_NO_EXISTS = 0x10;
// result codes:
const int FAILED_CONDITION = 0x4000000;
const int FINISHED_OPERATION = 0x1000000;
const double ANY_VERSION = 3.542694326329068e-103; // special marker for any version


WriteWorker::~WriteWorker() {
	// TODO: Make sure this runs on the JS main thread, or we need to move it
	if (envForTxn->writeWorker == this)
		envForTxn->writeWorker = nullptr;
}

WriteWorker::WriteWorker(transactional_splinterdb* db, DbWrap* envForTxn, uint32_t* instructions)
		: envForTxn(envForTxn),
		instructions(instructions),
		db(db) {
	//fprintf(stdout, "nextCompressibleArg %p\n", nextCompressibleArg);
		interruptionStatus = 0;
		txn = nullptr;
	}

AsyncWriteWorker::AsyncWriteWorker(transactional_splinterdb* db, DbWrap* envForTxn, uint32_t* instructions, const Function& callback)
		: WriteWorker(db, envForTxn, instructions), AsyncProgressWorker(callback, "splinterdb:write") {
	//fprintf(stdout, "nextCompressibleArg %p\n", nextCompressibleArg);
		interruptionStatus = 0;
		txn = nullptr;
	}

void AsyncWriteWorker::Execute(const AsyncProgressWorker::ExecutionProgress& execution) {
	executionProgress = (AsyncProgressWorker::ExecutionProgress*) &execution;
	Write();
}
void WriteWorker::SendUpdate() {
	fprintf(stderr, "This SendUpdate does not work!\n");
}
void AsyncWriteWorker::SendUpdate() {
	executionProgress->Send(nullptr, 0);
}
transaction* WriteWorker::AcquireTxn(int* flags) {
	bool commitSynchronously = *flags & TXN_SYNCHRONOUS_COMMIT;
	
	// TODO: if the conditionDepth is 0, we could allow the current worker's txn to be continued, committed and restarted
	pthread_mutex_lock(envForTxn->writingLock);
	retry:
	if (commitSynchronously && interruptionStatus == WORKER_WAITING) {
		//fprintf(stderr, "acquire interupting lock %p %u\n", this, commitSynchronously);
		interruptionStatus = INTERRUPT_BATCH;
		pthread_cond_signal(envForTxn->writingCond);
		pthread_cond_wait(envForTxn->writingCond, envForTxn->writingLock);
		if (interruptionStatus == RESTART_WORKER_TXN) {
			*flags |= TXN_FROM_WORKER;
			return nullptr;
		} else {
			interruptionStatus = WORKER_WAITING;
			goto retry;
		}
	} else {
		//if (interruptionStatus == RESTART_WORKER_TXN)
		//	pthread_cond_wait(envForTxn->writingCond, envForTxn->writingLock);
		interruptionStatus = USER_HAS_LOCK;
		*flags |= TXN_FROM_WORKER;
		//if (txn)
			//fprintf(stderr, "acquire lock from worker %p %u\n", txn, commitSynchronously);
		return txn;
	}
}

void WriteWorker::UnlockTxn() {
	//fprintf(stderr, "release txn %u\n", interruptionStatus);
	interruptionStatus = 0;
	pthread_cond_signal(envForTxn->writingCond);
	pthread_mutex_unlock(envForTxn->writingLock);
}
void WriteWorker::ReportError(const char* error) {
	hasError = true;
	fprintf(stderr, "Error %s\n", error);
}
void AsyncWriteWorker::ReportError(const char* error) {
	hasError = true;
	SetError(error);
}
int WriteWorker::WaitForCallbacks(transaction* txn, bool allowCommit, uint32_t* target) {
	int rc;
	//fprintf(stderr, "wait for callback %p\n", target);
	if (!finishedProgress)
		SendUpdate();
	pthread_cond_signal(envForTxn->writingCond);
	interruptionStatus = WORKER_WAITING;
	if (target) {
		uint64_t delay = 1;
		do {
			cond_timedwait(envForTxn->writingCond, envForTxn->writingLock, delay);
			delay = delay << 1ll;
			//if (delay > 500)
				//fprintf(stderr, "waited, %llu %p\n", delay, *target);
			if ((*target & 0xf) || (allowCommit && finishedProgress)) {
				// we are in position to continue writing or commit, so forward progress can be made without interrupting yet
				interruptionStatus = 0;
				return 0;
			}
		} while(interruptionStatus != INTERRUPT_BATCH);
	} else
		pthread_cond_wait(envForTxn->writingCond, envForTxn->writingLock);
	if (interruptionStatus == INTERRUPT_BATCH) { // interrupted by JS code that wants to run a synchronous transaction
	//	fprintf(stderr, "Performing batch interruption %u\n", allowCommit);
		interruptionStatus = RESTART_WORKER_TXN;
		rc = transactional_splinterdb_commit(db, txn);
		if (rc == 0) {
			// wait again until the sync transaction is completed
			//fprintf(stderr, "Waiting after interruption\n");
			this->txn = txn = nullptr;
			pthread_cond_signal(envForTxn->writingCond);
			pthread_cond_wait(envForTxn->writingCond, envForTxn->writingLock);
			// now restart our transaction
			rc = transactional_splinterdb_begin(db, txn);
			this->txn = txn;
			//fprintf(stderr, "Restarted txn after interruption\n");
			interruptionStatus = 0;
		}
		if (rc != 0) {
			fprintf(stdout, "wfc unlock due to error %u\n", rc);
			return rc;
		}
	} else
		interruptionStatus = 0;
	return 0;
}
int WriteWorker::DoWrites(transaction* txn, DbWrap* envForTxn, uint32_t* instruction, WriteWorker* worker) {
	slice key;
	slice value;
	int rc = 0;
	int conditionDepth = 0;
	int validatedDepth = 0;
	double conditionalVersion, setVersion = 0;
	bool overlappedWord = !!worker;
	uint32_t* start;
	transactional_splinterdb* db = envForTxn->db;
	do {
next_inst:	start = instruction++;
		uint32_t flags = *start;
		int dbi = 0;
		bool validated = conditionDepth == validatedDepth;
		if (flags & 0xf0c0) {
			fprintf(stderr, "Unknown flag bits %u %p\n", flags, start);
			fprintf(stderr, "flags after message %u\n", *start);
			worker->ReportError("Unknown flags\n");
			return 0;
		}
		if (flags & HAS_KEY) {
			// a key based instruction, get the key
			dbi = (int) *instruction++;
			key.length = *instruction++;
			key.data = instruction;
			instruction = (uint32_t*) (((size_t) instruction + key.length + 16) & (~7));
			if (flags & HAS_VALUE) {
				if (flags & COMPRESSIBLE) {
					int64_t status = -1;
					status = std::atomic_exchange((std::atomic<int64_t>*)(instruction + 2), (int64_t)1);
					if (status == 2) {
						//fprintf(stderr, "wait on compression %p\n", instruction);
						worker->interruptionStatus = WORKER_WAITING;
						do {
							pthread_cond_wait(envForTxn->writingCond, envForTxn->writingLock);
						} while (std::atomic_load((std::atomic<int64_t>*)(instruction + 2)));
						worker->interruptionStatus = 0;
					} else if (status > 2) {
						//fprintf(stderr, "doing the compression ourselves\n");
						((Compression*) (size_t) *((double*)&status))->compressInstruction(nullptr, (double*) (instruction + 2));
					} // else status is 0 and compression is done
					// compressed
					value.data = (void*)(size_t) * ((size_t*)instruction);
					if ((size_t)value.data > 0x1000000000000)
						fprintf(stderr, "compression not completed %p %i\n", value.data, (int) status);
					value.length = *(instruction - 1);
					instruction += 4; // skip compression pointers
				} else {
					value.data = (void*)(size_t) * ((double*)instruction);
					value.length = *(instruction - 1);
					instruction += 2;
				}
			}
			if (flags & CONDITIONAL_VERSION) {
				conditionalVersion = *((double*) instruction);
				instruction += 2;
				slice conditionalValue;
				rc = 0;//transactional_splinterdb_lookup(txn, dbi, &key, &conditionalValue);
				if (rc) {
				    // not found counts as version 0, so this is acceptable for conditional less than,
				    // otherwise does not validate
                    validated = rc == 0;//MDB_NOTFOUND && (flags & CONDITIONAL_ALLOW_NOTFOUND);
				} else if (conditionalVersion != ANY_VERSION) {
					double version;
					memcpy(&version, conditionalValue.data, 8);
					validated = validated && ((flags & CONDITIONAL_VERSION_LESS_THAN) ? version <= conditionalVersion : (version == conditionalVersion));
				}
			}
			if (flags & SET_VERSION) {
				setVersion = *((double*) instruction);
				instruction += 2;
			}
			if ((flags & IF_NO_EXISTS) && (flags & START_CONDITION_BLOCK)) {
				rc = 0;// mdb_get(txn, dbi, &key, &value);
				validated = validated && rc == 0;//MDB_NOTFOUND;
			}
		} else
			instruction++;
		//fprintf(stderr, "instr flags %p %p %u\n", start, flags, conditionDepth);
		if (validated || !(flags & CONDITIONAL)) {
			switch (flags & 0xf) {
			case NO_INSTRUCTION_YET:
				instruction -= 2; // reset back to the previous flag as the current instruction
				rc = 0;
				// in windows InterlockedCompareExchange might be faster
				if (!worker->finishedProgress || conditionDepth) {
					if (std::atomic_compare_exchange_strong((std::atomic<uint32_t>*) start,
							(uint32_t*) &flags,
							(uint32_t)WAITING_OPERATION)) {
						worker->WaitForCallbacks(txn, conditionDepth == 0, start);
					}
					goto next_inst;
				} else {
					if (std::atomic_compare_exchange_strong((std::atomic<uint32_t>*) start,
							(uint32_t*) &flags,
							(uint32_t)TXN_DELIMITER)) {
						worker->instructions = start;
						return 0;
					} else
						goto next_inst;						
				}
			case BLOCK_END:
				conditionDepth--;
				if (validatedDepth > conditionDepth)
					validatedDepth--;
				if (conditionDepth < 0) {
					fprintf(stderr, "Negative condition depth");
				}
				goto next_inst;
			case PUT:
				if (flags & SET_VERSION)
					rc = putWithVersion(db, txn, key, value, flags, setVersion);
				else
					rc = transactional_splinterdb_insert(db, txn, key, value);
				if (flags & COMPRESSIBLE)
					free((void*) value.data);
				//fprintf(stdout, "put %u \n", key.length);
				break;
			case DEL:
				rc = transactional_splinterdb_delete(db, txn, key);
				break;
			case DEL_VALUE:
				rc = transactional_splinterdb_delete(db, txn, key);
				if (flags & COMPRESSIBLE)
					free((void*) value.data);
				break;
			case START_BLOCK: case START_CONDITION_BLOCK:
				rc = 0;//validated ? 0 : MDB_NOTFOUND;
				if (validated)
					validatedDepth++;
				conditionDepth++;
				break;
			case USER_CALLBACK:
				worker->finishedProgress = false;
				worker->progressStatus = 2;
				rc = 0;
				if (flags & USER_CALLBACK_STRICT_ORDER) {
					std::atomic_fetch_or((std::atomic<uint32_t>*) start, (uint32_t) FINISHED_OPERATION); // mark it as finished so it is processed
					while (!worker->finishedProgress) {
						worker->WaitForCallbacks(txn, conditionDepth == 0, nullptr);
					}
				}
				break;
			case DROP_DB:
				//rc = mdb_drop(txn, dbi, (flags & DELETE_DATABASE) ? 1 : 0);
				break;
			case POINTER_NEXT:
				instruction = (uint32_t*)(size_t) * ((double*)instruction);
				goto next_inst;
			default:
				fprintf(stderr, "Unknown flags %u %p\n", flags, start);
				fprintf(stderr, "flags after message %u\n", *start);
				worker->ReportError("Unknown flags\n");
				return 22;
			}
			if (rc) {
				/*if (!(rc == MDB_KEYEXIST || rc == MDB_NOTFOUND)) {
					if (worker) {
						worker->ReportError("error in splinterdb");
					} else {
						return rc;
					}
				}*/
				flags = FINISHED_OPERATION | FAILED_CONDITION;
			}
			else
				flags = FINISHED_OPERATION;
		} else
			flags = FINISHED_OPERATION | FAILED_CONDITION;
		//fprintf(stderr, "finished flag %p\n", flags);
		if (overlappedWord) {
			std::atomic_fetch_or((std::atomic<uint32_t>*) start, flags);
			overlappedWord = false;
		} else
			*start |= flags;
	} while(worker); // keep iterating in async/multiple-instruction mode, just one instruction in sync mode
	return rc;
}

const int READER_CHECK_INTERVAL = 600; // ten minutes
void WriteWorker::Write() {
	int rc;
	finishedProgress = true;
	unsigned int envFlags;
	pthread_mutex_lock(envForTxn->writingLock);
	#ifndef _WIN32
	int retries = 0;
	retry:
	#endif
	txn = new transaction;
	rc = transactional_splinterdb_begin(db, txn);
	if (rc != 0) {
		return ReportError("error in splinterdb");
	}
	hasError = false;
	rc = DoWrites(txn, envForTxn, instructions, this);
	uint32_t txnId = 0;// (uint32_t) transaction_id(txn);
	if (rc || hasError)
		transactional_splinterdb_abort(db, txn);
	else
		rc = transactional_splinterdb_commit(db, txn);
	txn = nullptr;
	pthread_mutex_unlock(envForTxn->writingLock);
	if (rc || hasError) {
		std::atomic_fetch_or((std::atomic<uint32_t>*) instructions, (uint32_t) TXN_HAD_ERROR);
		if (rc)
			ReportError("error in splinterdb");
		return;
	}
	*(instructions + 1) = txnId;
	std::atomic_fetch_or((std::atomic<uint32_t>*) instructions, (uint32_t) TXN_COMMITTED);
}

void AsyncWriteWorker::OnProgress(const char* data, size_t count) {
	if (progressStatus == 1) {
		Callback().Call({ Number::New(Env(), progressStatus)});
		return;
	}
	if (finishedProgress)
		return;
	pthread_mutex_lock(envForTxn->writingLock);
	while(!txn) // possible to jump in after an interrupted txn here
		pthread_cond_wait(envForTxn->writingCond, envForTxn->writingLock);
	envForTxn->writeTxn = new TxnTracked(txn, 0);
	finishedProgress = true;
	napi_value result, arg; // we use direct napi call here because node-addon-api interface with throw a fatal error if a worker thread is terminating, and bun doesn't support escapable scopes yet
	napi_create_int32(Env(), progressStatus, &arg);
	napi_call_function(Env(), Env().Undefined(), Callback().Value(), 1, &arg, &result);
	delete envForTxn->writeTxn;
	envForTxn->writeTxn = nullptr;
	pthread_cond_signal(envForTxn->writingCond);
	pthread_mutex_unlock(envForTxn->writingLock);
}

void AsyncWriteWorker::OnOK() {
	finishedProgress = true;
	napi_value result, arg; // we use direct napi call here because node-addon-api interface with throw a fatal error if a worker thread is terminating, and bun doesn't support escapable scopes yet
	napi_create_int32(Env(), 0, &arg);
	napi_call_function(Env(), Env().Undefined(), Callback().Value(), 1, &arg, &result);
}

Value DbWrap::startWriting(const Napi::CallbackInfo& info) {
	if (!this->db) {
		return throwError(info.Env(), "The environment is already closed.");
	}
	size_t instructionAddress = info[0].As<Number>().Int64Value();
	AsyncWriteWorker* worker = new AsyncWriteWorker(this->db, this, (uint32_t*) instructionAddress, info[1].As<Function>());
	this->writeWorker = worker;
	worker->Queue();
	return info.Env().Undefined();
}

NAPI_FUNCTION(DbWrap::write) {
	ARGS(2)
	GET_INT64_ARG(0);
	DbWrap* ew = (DbWrap*) i64;
	if (!ew->db) {
		napi_throw_error(env, nullptr, "The environment is already closed.");
		RETURN_UNDEFINED;
	}
	
	napi_get_value_int64(env, args[1], &i64);
	uint32_t* instructionAddress = (uint32_t*) i64;
	int rc = 0;
	if (instructionAddress)
		rc = WriteWorker::DoWrites(&ew->txn, ew, instructionAddress, nullptr);
	else if (ew->writeWorker) {
		pthread_cond_signal(ew->writingCond);
	}
	/*if (rc && !(rc == MDB_KEYEXIST || rc == MDB_NOTFOUND)) {
		throwLmdbError(env, rc);
		RETURN_UNDEFINED;
	}*/
	RETURN_UNDEFINED;
}
