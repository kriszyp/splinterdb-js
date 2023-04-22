#include "splinterdb-js.h"
#include <string.h>

using namespace Napi;

const int INCLUDE_VALUES = 0x100;
const int REVERSE = 0x400;
const int VALUES_FOR_KEY = 0x800;
const int ONLY_COUNT = 0x1000;
const int RENEW_CURSOR = 0x2000;
const int EXACT_MATCH = 0x4000;
const int INCLUSIVE_END = 0x8000;
const int EXCLUSIVE_START = 0x10000;

IteratorWrap::IteratorWrap(const CallbackInfo& info) : Napi::ObjectWrap<IteratorWrap>(info) {
	this->keyType = LmdbKeyType::StringKey;
	this->freeKey = nullptr;
	this->endKey.length = 0; // indicates no end key (yet)
	if (info.Length() < 1) {
		throwError(info.Env(), "Wrong number of arguments");
		return;
	}

	DbWrap *dw;
	napi_unwrap(info.Env(), info[0], (void**)&dw);

	// Open the iterator
	splinterdb_iterator *iterator;
	transaction *txn = dw->ew->getReadTxn();
	int rc = splinterdb_iterator_init(txn, dw->db->kvsb, &iterator);
	if (rc != 0) {
		throwLmdbError(info.Env(), rc);
		return;
	}
	info.This().As<Object>().Set("address", Number::New(info.Env(), (size_t) this));
	this->iterator = iterator;
	this->dw = dw;
	this->txn = txn;
	this->keyType = keyType;
}

IteratorWrap::~IteratorWrap() {
	if (this->iterator) {
		// Don't close iterator here, it is possible that the environment may already be closed, which causes it to crash
		//splinterdb_iterator_close(this->iterator);
	}
	if (this->freeKey) {
		this->freeKey(this->key);
	}
}

Value IteratorWrap::close(const CallbackInfo& info) {
	if (!this->iterator) {
	  return throwError(info.Env(), "iterator.close: Attempt to close a closed iterator!");
	}
	splinterdb_iterator_close(this->iterator);
	this->iterator = nullptr;
	return info.Env().Undefined();
}

Value IteratorWrap::del(const CallbackInfo& info) {
	int flags = 0;

	if (info.Length() == 1) {
		if (!info[0].IsObject()) {
			return throwError(info.Env(), "iterator.del: Invalid options argument. It should be an object.");
		}
		
		auto options = info[0].As<Object>();
		setFlagFromValue(&flags, splinterdb_NODUPDATA, "noDupData", false, options);
	}

	int rc = splinterdb_iterator_del(this->iterator, flags);
	if (rc != 0) {
		return throwLmdbError(info.Env(), rc);
	}
	return info.Env().Undefined();
}
int IteratorWrap::returnEntry(int lastRC, slice &key, slice &data) {
	if (lastRC) {
		if (lastRC == splinterdb_NOTFOUND)
			return 0;
		else {
			return lastRC > 0 ? -lastRC : lastRC;
		}
	}
	if (endKey.length > 0) {
		int comparison;
		if (flags & VALUES_FOR_KEY)
			comparison = splinterdb_dcmp(txn, dw->dbi, &endKey, &data);
		else
			comparison = splinterdb_cmp(txn, dw->dbi, &endKey, &key);
		if ((flags & REVERSE) ? comparison >= 0 : (comparison <= 0)) {
			if (!((flags & INCLUSIVE_END) && comparison == 0))
				return 0;
		}
	}
	char* keyBuffer = dw->ew->keyBuffer;
	if (flags & INCLUDE_VALUES) {
		int result = getVersionAndUncompress(data, dw);
		bool fits = true;
		if (result) {
			fits = valToBinaryFast(data, dw); // it fit in the global/compression-target buffer
		}
		if (fits || result == 2 || data.length < SHARED_BUFFER_THRESHOLD) {// if it was decompressed
			*((uint32_t*)keyBuffer) = data.length;
			*((uint32_t*)(keyBuffer + 4)) = 0; // buffer id of 0
		} else {
			DbWrap::toSharedBuffer(dw->ew->env, (uint32_t*) dw->ew->keyBuffer, data);
		}
	}
	if (!(flags & VALUES_FOR_KEY)) {
		memcpy(keyBuffer + 32, key.data, key.length);
		*(keyBuffer + 32 + key.length) = 0; // make sure it is null terminated for the sake of better ordered-binary performance
	}

	return key.length;
}

const int START_ADDRESS_POSITION = 4064;
int32_t IteratorWrap::doPosition(uint32_t offset, uint32_t keySize, uint64_t endKeyAddress) {
	//char* keyBuffer = dw->ew->keyBuffer;
	slice key, data;
	int rc;
	if (flags & RENEW_CURSOR) { // TODO: check the txn_id to determine if we need to renew
		rc = splinterdb_iterator_renew(txn = dw->ew->getReadTxn(), iterator);
		if (rc) {
			if (rc > 0)
				rc = -rc;
			return rc;
		}
	}
	if (endKeyAddress) {
		uint32_t* keyBuffer = (uint32_t*) endKeyAddress;
		endKey.length = *keyBuffer;
		endKey.data = (char*)(keyBuffer + 1);
	} else
		endKey.length = 0;
	iteratingOp = (flags & REVERSE) ?
		(flags & INCLUDE_VALUES) ?
			(flags & VALUES_FOR_KEY) ? splinterdb_PREV_DUP : splinterdb_PREV :
			splinterdb_PREV_NODUP :
		(flags & INCLUDE_VALUES) ?
			(flags & VALUES_FOR_KEY) ? splinterdb_NEXT_DUP : splinterdb_NEXT :
			splinterdb_NEXT_NODUP;
	key.length = keySize;
	key.data = dw->ew->keyBuffer;
	if (keySize == 0) {
		rc = splinterdb_iterator_get(iterator, &key, &data, flags & REVERSE ? splinterdb_LAST : splinterdb_FIRST);  
	} else {
		if (flags & VALUES_FOR_KEY) { // only values for this key
			// take the next part of the key buffer as a pointer to starting data
			uint32_t* startValueBuffer = (uint32_t*)(size_t)(*(double*)(dw->ew->keyBuffer + START_ADDRESS_POSITION));
			data.length = endKeyAddress ? *((uint32_t*)startValueBuffer) : 0;
			data.data = startValueBuffer + 1;
			slice startValue;
			if (flags & EXCLUSIVE_START)
				startValue = data; // save it for comparison
			if (flags & REVERSE) {// reverse through values
				startValue = data; // save it for comparison
				rc = splinterdb_iterator_get(iterator, &key, &data, data.length ? splinterdb_GET_BOTH_RANGE : splinterdb_SET_KEY);
				if (rc) {
					if (startValue.length) {
						// value specified, but not found, so find key and go to last item
						rc = splinterdb_iterator_get(iterator, &key, &data, splinterdb_SET_KEY);
						if (!rc)
							rc = splinterdb_iterator_get(iterator, &key, &data, splinterdb_LAST_DUP);
					} // else just couldn't find the key
				} else { // found entry
					if (startValue.length == 0) // no value specified, so go to last value
						rc = splinterdb_iterator_get(iterator, &key, &data, splinterdb_LAST_DUP);
					else if (splinterdb_dcmp(txn, dw->dbi, &startValue, &data)) // the range found the next value *after* the start
						rc = splinterdb_iterator_get(iterator, &key, &data, splinterdb_PREV_DUP);
				}
			} else // forward, just do a get by range
				rc = splinterdb_iterator_get(iterator, &key, &data, data.length ?
					(flags & EXACT_MATCH) ? splinterdb_GET_BOTH : splinterdb_GET_BOTH_RANGE : splinterdb_SET_KEY);

			if (rc == splinterdb_NOTFOUND)
				return 0;
			if (flags & ONLY_COUNT && (!endKeyAddress || (flags & EXACT_MATCH))) {
				size_t count;
				rc = splinterdb_iterator_count(iterator, &count);
				if (rc)
					return rc > 0 ? -rc : rc;
				return count;
			}
			if (flags & EXCLUSIVE_START) {
				while(!rc) {
					if (splinterdb_dcmp(txn, dw->dbi, &startValue, &data))
						break;
					rc = splinterdb_iterator_get(iterator, &key, &data, iteratingOp);
				}
			}
		} else {
			slice firstKey;
			if (flags & EXCLUSIVE_START)
				firstKey = key; // save it for comparison
			if (flags & REVERSE) {// reverse
				firstKey = key; // save it for comparison
				rc = splinterdb_iterator_get(iterator, &key, &data, splinterdb_SET_RANGE);
				if (rc)
					rc = splinterdb_iterator_get(iterator, &key, &data, splinterdb_LAST);
				else if (splinterdb_cmp(txn, dw->dbi, &firstKey, &key)) // the range found the next entry *after* the start
					rc = splinterdb_iterator_get(iterator, &key, &data, splinterdb_PREV);
				else if (dw->flags & splinterdb_DUPSORT)
					// we need to go to the last value of this key
					rc = splinterdb_iterator_get(iterator, &key, &data, splinterdb_LAST_DUP);
			} else // forward, just do a get by range
				rc = splinterdb_iterator_get(iterator, &key, &data, (flags & EXACT_MATCH) ? splinterdb_SET_KEY : splinterdb_SET_RANGE);
			if (flags & EXCLUSIVE_START) {
				while(!rc) {
					if (splinterdb_cmp(txn, dw->dbi, &firstKey, &key))
						break;
					rc = splinterdb_iterator_get(iterator, &key, &data, iteratingOp);
				}
			}
		}
	}

	while (offset-- > 0 && !rc) {
		rc = splinterdb_iterator_get(iterator, &key, &data, iteratingOp);
	}
	if (flags & ONLY_COUNT) {
		uint32_t count = 0;
		bool useIteratorCount = false;
		// if we are in a dupsort database, and we are iterating over all entries, we can just count all the values for each key
		if (dw->flags & splinterdb_DUPSORT) {
			if (iteratingOp == splinterdb_PREV) {
				iteratingOp = splinterdb_PREV_NODUP;
				useIteratorCount = true;
			}
			if (iteratingOp == splinterdb_NEXT) {
				iteratingOp = splinterdb_NEXT_NODUP;
				useIteratorCount = true;
			}
		}

		while (!rc) {
			if (endKey.length > 0) {
				int comparison;
				if (flags & VALUES_FOR_KEY)
					comparison = splinterdb_dcmp(txn, dw->dbi, &endKey, &data);
				else
					comparison = splinterdb_cmp(txn, dw->dbi, &endKey, &key);
				if ((flags & REVERSE) ? comparison >= 0 : (comparison <=0)) {
					if (!((flags & INCLUSIVE_END) && comparison == 0))
						return count;
				}
			}
			if (useIteratorCount) {
				size_t countForKey;
				rc = splinterdb_iterator_count(iterator, &countForKey);
				if (rc) {
					if (rc > 0)
						rc = -rc;
					return rc;
				}
				count += countForKey;
			} else
				count++;
			rc = splinterdb_iterator_get(iterator, &key, &data, iteratingOp);
		}
		return count;
	}
	// TODO: Handle count?
	return returnEntry(rc, key, data);
}
NAPI_FUNCTION(position) {
	ARGS(5)
    GET_INT64_ARG(0);
    IteratorWrap* cw = (IteratorWrap*) i64;
	GET_UINT32_ARG(cw->flags, 1);
	uint32_t offset;
	GET_UINT32_ARG(offset, 2);
	uint32_t keySize;
	GET_UINT32_ARG(keySize, 3);
    napi_get_value_int64(env, args[4], &i64);
    int64_t endKeyAddress = i64;
	int32_t result = cw->doPosition(offset, keySize, endKeyAddress);
	RETURN_INT32(result);
}
int32_t positionFFI(double cwPointer, uint32_t flags, uint32_t offset, uint32_t keySize, uint64_t endKeyAddress) {
	IteratorWrap* cw = (IteratorWrap*) (size_t) cwPointer;
	DbWrap* dw = cw->dw;
	dw->getFast = true;
	cw->flags = flags;
	return cw->doPosition(offset, keySize, endKeyAddress);
}

NAPI_FUNCTION(iterate) {
	ARGS(1)
    GET_INT64_ARG(0);
    IteratorWrap* cw = (IteratorWrap*) i64;
	slice key, data;
	int rc = splinterdb_iterator_get(cw->iterator, &key, &data, cw->iteratingOp);
	RETURN_INT32(cw->returnEntry(rc, key, data));
}

int32_t iterateFFI(double cwPointer) {
	IteratorWrap* cw = (IteratorWrap*) (size_t) cwPointer;
	DbWrap* dw = cw->dw;
	dw->getFast = true;
	slice key, data;
	int rc = splinterdb_iterator_get(cw->iterator, &key, &data, cw->iteratingOp);
	return cw->returnEntry(rc, key, data);
}


NAPI_FUNCTION(getCurrentValue) {
	ARGS(1)
    GET_INT64_ARG(0);
    IteratorWrap* cw = (IteratorWrap*) i64;
	slice key, data;
	int rc = splinterdb_iterator_get(cw->iterator, &key, &data, splinterdb_GET_CURRENT);
	RETURN_INT32(cw->returnEntry(rc, key, data));
}

napi_finalize noopIterator = [](napi_env, void *, void *) {
	// Data belongs to Lsplinterdb, we shouldn't free it here
};
NAPI_FUNCTION(getCurrentShared) {
	ARGS(1)
    GET_INT64_ARG(0);
    IteratorWrap* cw = (IteratorWrap*) i64;
	slice key, data;
	int rc = splinterdb_iterator_get(cw->iterator, &key, &data, splinterdb_GET_CURRENT);
	if (rc)
		RETURN_INT32(cw->returnEntry(rc, key, data));
	getVersionAndUncompress(data, cw->dw);
	napi_create_external_buffer(env, data.length,
		(char*) data.data, noopIterator, nullptr, &returnValue);
	return returnValue;
}

NAPI_FUNCTION(renew) {
	ARGS(1)
    GET_INT64_ARG(0);
    IteratorWrap* cw = (IteratorWrap*) i64;
	splinterdb_iterator_renew(cw->txn = cw->dw->ew->getReadTxn(), cw->iterator);
	RETURN_UNDEFINED;
}

void IteratorWrap::setupExports(Napi::Env env, Object exports) {
	// IteratorWrap: Prepare constructor template
	Function IteratorClass = DefineClass(env, "Iterator", {
	// IteratorWrap: Add functions to the prototype
		IteratorWrap::InstanceMethod("close", &IteratorWrap::close),
		IteratorWrap::InstanceMethod("del", &IteratorWrap::del),
	});
	EXPORT_NAPI_FUNCTION("position", position);
	EXPORT_NAPI_FUNCTION("iterate", iterate);
	EXPORT_NAPI_FUNCTION("getCurrentValue", getCurrentValue);
	EXPORT_NAPI_FUNCTION("getCurrentShared", getCurrentShared);
	EXPORT_NAPI_FUNCTION("renew", renew);
	EXPORT_FUNCTION_ADDRESS("positionPtr", positionFFI);
	EXPORT_FUNCTION_ADDRESS("iteratePtr", iterateFFI);

	exports.Set("Iterator", IteratorClass);

//	iteratorTpl->InstanceTemplate()->SetInternalFieldCount(1);
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

