#include "splinterdb-js.h"

using namespace Napi;

TxnTracked::TxnTracked(transaction *txn, unsigned int flags) {
	this->txn = txn;
	this->flags = flags;
	parent = nullptr;
}

TxnTracked::~TxnTracked() {
	this->txn = nullptr;
}

TxnWrap::TxnWrap(const Napi::CallbackInfo& info) : ObjectWrap<TxnWrap>(info) {
	DbWrap *ew;
	db = ew->db;
	napi_unwrap(info.Env(), info[0], (void**)&ew);
	int flags = 0;
	TxnWrap *parentTw;
	if (info[1].IsBoolean() && ew->writeWorker) { // this is from a transaction callback
		//txn = ew->writeWorker->AcquireTxn(&flags);
		parentTw = nullptr;
	} else {
		if (info[1].IsObject()) {
			Object options = info[1].As<Object>();

			// Get flags from options

			setFlagFromValue(&flags, 1, "readOnly", false, options);
		} else if (info[1].IsNumber()) {
			flags = info[1].As<Number>();
		}
		transaction *parentTxn;
		if (info[2].IsObject()) {
			napi_unwrap(info.Env(), info[2], (void**) &parentTw);
		//	parentTxn = parentTw->txn;
		} else {
			parentTxn = nullptr;
			parentTw = nullptr;
		}
		//fprintf(stderr, "txn_begin from txn.cpp %u %p\n", flags, parentTxn);
		/*if ((flags & MDB_RDONLY) && parentTxn) {
			// if a txn is passed in, we check to see if it is up-to-date and can be reused
			MDB_envinfo stat;
			mdb_env_info(ew->env, &stat);
			if (transaction_id(parentTxn) == stat.me_last_txnid) {
				txn = nullptr;
				info.This().As<Object>().Set("address", Number::New(info.Env(), 0));
				return;
			}
			parentTxn = nullptr;
		}*/
		int rc = transactional_splinterdb_begin(db, &txn);
		if (rc != 0) {
		//	txn = nullptr;
//			throwLmdbError(info.Env(), rc);
			return;
		}
	}

	// Set the current write transaction
/*	if (0 == (flags & MDB_RDONLY)) {
		ew->currentWriteTxn = this;
	}
	else {*/
		ew->readTxns.push_back(this);
		ew->currentReadTxn = &txn;
	//}
	this->parentTw = parentTw;
	this->flags = flags;
	this->ew = ew;
	this->db = ew->db;
	info.This().As<Object>().Set("address", Number::New(info.Env(), (size_t) this));
}

TxnWrap::~TxnWrap() {
	// Close if not closed already
	//if (this->txn) {
		transactional_splinterdb_abort(db, &txn);
		this->removeFromDbWrap();
	//}
}

void TxnWrap::removeFromDbWrap() {
	if (this->ew) {
		if (this->ew->currentWriteTxn == this) {
			this->ew->currentWriteTxn = this->parentTw;
		}
		else {
			auto it = std::find(ew->readTxns.begin(), ew->readTxns.end(), this);
			if (it != ew->readTxns.end()) {
				ew->readTxns.erase(it);
			}
		}
		this->ew = nullptr;
	}
	//this->txn = nullptr;
}

Value TxnWrap::commit(const Napi::CallbackInfo& info) {
	/*if (!this->txn) {
		return throwError(info.Env(), "The transaction is already closed.");
	}*/
	int rc;
	WriteWorker* writeWorker = this->ew->writeWorker;
	if (writeWorker) {
		// if (writeWorker->txn && env->writeMap)
		// rc = 0
		// else
		rc = transactional_splinterdb_commit(db, &txn);
		
		pthread_mutex_unlock(this->ew->writingLock);
	}
	else
		rc = transactional_splinterdb_commit(db, &txn);
	//fprintf(stdout, "commit done\n");
	this->removeFromDbWrap();

	if (rc != 0) {
		return throwLmdbError(info.Env(), rc);
	}
	return info.Env().Undefined();
}

Value TxnWrap::abort(const Napi::CallbackInfo& info) {

	transactional_splinterdb_abort(db, &txn);
	this->removeFromDbWrap();
	return info.Env().Undefined();
}
NAPI_FUNCTION(resetTxn) {
	ARGS(1)
	GET_INT64_ARG(0);
	TxnWrap* tw = (TxnWrap*) i64;
	tw->reset();
	RETURN_UNDEFINED;
}
void resetTxnFFI(double twPointer) {
	TxnWrap* tw = (TxnWrap*) (size_t) twPointer;
	tw->reset();
}

void TxnWrap::setupExports(Napi::Env env, Object exports) {
		// TxnWrap: Prepare constructor template
	Function TxnClass = DefineClass(env, "Txn", {
		// TxnWrap: Add functions to the prototype
		TxnWrap::InstanceMethod("commit", &TxnWrap::commit),
		TxnWrap::InstanceMethod("abort", &TxnWrap::abort),
	});
	exports.Set("Txn", TxnClass);
	//txnTpl->InstanceTemplate()->SetInternalFieldCount(1);
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
