import { RangeIterable }  from './util/RangeIterable.js';
import { getAddress, Cursor, Txn, orderedBinary, lmdbError, getByBinary, detachBuffer, setGlobalBuffer, prefetch, iterate, position as doPosition, resetTxn, getCurrentValue, getCurrentShared, getStringByBinary, globalBuffer, getSharedBuffer } from './native.js';
import { saveKey }  from './keys.js';
const ITERATOR_DONE = { done: true, value: undefined };
const Uint8ArraySlice = Uint8Array.prototype.slice;
const Uint8A = typeof Buffer != 'undefined' ? Buffer.allocUnsafeSlow : Uint8Array
let getValueBytes = globalBuffer;
if (!getValueBytes.maxLength) {
	getValueBytes.maxLength = getValueBytes.length;
	getValueBytes.isGlobal = true;
	Object.defineProperty(getValueBytes, 'length', { value: getValueBytes.length, writable: true, configurable: true });
}
const START_ADDRESS_POSITION = 4064;
const NEW_BUFFER_THRESHOLD = 0x8000;
export const UNMODIFIED = {};

let unreadResolution, lastQueuedResolution, nextResolution;

export function addReadMethods(LMDBStore, {
	maxKeySize, env, keyBytes, keyBytesView, getLastVersion, getLastTxnId
}) {
	let readTxn, readTxnRenewed, asSafeBuffer = false;
	let renewId = 1;
	let mmaps = [];
	Object.assign(LMDBStore.prototype, {
		getString(id) {
			(env.writeTxn || (readTxnRenewed ? readTxn : renewReadTxn(this)));
			let string = getStringByBinary(this.dbAddress, this.writeKey(id, keyBytes, 0));
			if (typeof string === 'number') { // indicates the buffer wasn't large enough
				this._allocateGetBuffer(string);
				// and then try again
				string = getStringByBinary(this.dbAddress, this.writeKey(id, keyBytes, 0));
			}
			if (string)
				this.lastSize = string.length;
			return string;
		},
		getBinaryFast(id, options) {
			let rc;
			if (options?.txn?.address)
				rc = this.lastSize = getByBinary(env.address, this.writeKey(id, keyBytes, 0), options.ifNotTxnId || 0, options.txn.address);
			else {
				(env.writeTxn || (readTxnRenewed ? readTxn : renewReadTxn(this)));
				rc = this.lastSize = getByBinary(env.address, this.writeKey(id, keyBytes, 0), options?.ifNotTxnId || 0, 0);
			}
			if (rc < 0) {
				if (rc == -30798) // MDB_NOTFOUND
					return; // undefined
				if (rc == -30004) // txn id matched
					return UNMODIFIED;
				if (rc == -30781 /*MDB_BAD_VALSIZE*/ && this.writeKey(id, keyBytes, 0) == 0)
					throw new Error(id === undefined ?
					'A key is required for get, but is undefined' :
					'Zero length key is not allowed in LMDB');
				if (rc == -30000) // int32 overflow, read uint32
					rc = this.lastSize = keyBytesView.getUint32(0, true);
				else if (rc == -30001) {// shared buffer
					this.lastSize = keyBytesView.getUint32(0, true);
					let bufferId = keyBytesView.getUint32(4, true);
					return getMMapBuffer(bufferId, this.lastSize);
				} else
					throw lmdbError(rc);
			}
			let compression = this.compression;
			let bytes = compression ? compression.getValueBytes : getValueBytes;
			if (rc > bytes.maxLength) {
				// this means the target buffer wasn't big enough, so the get failed to copy all the data from the database, need to either grow or use special buffer
				return this._returnLargeBuffer(
					() => getByBinary(this.dbAddress, this.writeKey(id, keyBytes, 0), 0, 0));
			}
			bytes.length = this.lastSize;
			return bytes;
		},
		getBFAsync(id, callback, options) {
			let txn = options.txn || (env.writeTxn || (readTxnRenewed ? readTxn : renewReadTxn(this)));
			let address = recordReadInstruction(txn.address, this.db.dbi, id, this.writeKey, maxKeySize, ({ bufferId, offset, size }) => {
				let buffer = mmaps[bufferId];
				if (!buffer) {
					buffer = mmaps[bufferId] = getSharedBuffer(bufferId, env.address);
				}
				callback(buffer, offset, size);
			});
			if (address) {
				startReading();
			}
		},
		getAsync(id, options, callback) {
			let promise;
			if (!callback)
				promise = new Promise(resolve => callback = resolve);
			this.getBFAsync(id, options, (buffer, offset, size) => {
				let bytes = new Uint8Array(buffer, offset, size);
				if (this.encoding == 'binary')
					callback(bytes);
				else if (this.decoder) {
					// the decoder potentially uses the data from the buffer in the future and needs a stable buffer
					callback(bytes && this.decoder.decode(bytes));
				}
			});
			return promise;
		},
		retainBinary(buffer) {
			if (!buffer)
				return
			if (!buffer.isGlobal && !env.writeTxn) {
				buffer.txn = readTxn;
				readTxn.refCount = (readTxn.refCount || 0) + 1;
				return buffer;
			} else {
				return Uint8ArraySlice.call(buffer, 0, this.lastSize);
			}
		},
		_returnLargeBuffer(getFast) {
			let bytes;
			let compression = this.compression;
			if (asSafeBuffer && this.lastSize > NEW_BUFFER_THRESHOLD) {
				// used by getBinary to indicate it should create a dedicated buffer to receive this
				let bytesToRestore
				try {
					if (compression) {
						bytesToRestore = compression.getValueBytes;
						let dictionary = compression.dictionary || [];
						let dictLength = (dictionary.length >> 3) << 3;// make sure it is word-aligned
						bytes = makeReusableBuffer(this.lastSize);
						compression.setBuffer(bytes, this.lastSize, dictionary, dictLength);
						compression.getValueBytes = bytes;
					} else {
						bytesToRestore = getValueBytes;
						setGlobalBuffer(bytes = getValueBytes = makeReusableBuffer(this.lastSize));
					}
					getFast();
				} finally {
					if (compression) {
						let dictLength = (compression.dictionary.length >> 3) << 3;
						compression.setBuffer(bytesToRestore, bytesToRestore.maxLength, compression.dictionary, dictLength);
						compression.getValueBytes = bytesToRestore;
					} else {
						setGlobalBuffer(bytesToRestore);
						getValueBytes = bytesToRestore;
					}
				}
				return bytes;
			}
			// grow our shared/static buffer to accomodate the size of the data
			bytes = this._allocateGetBuffer(this.lastSize);
			// and try again
			getFast();
			bytes.length = this.lastSize;
			return bytes;
		},
		_allocateGetBuffer(lastSize) {
			let newLength = Math.min(Math.max(lastSize * 2, 0x1000), 0xfffffff8);
			let bytes;
			if (this.compression) {
				let dictionary = this.compression.dictionary || new Uint8A(0);
				let dictLength = (dictionary.length >> 3) << 3;// make sure it is word-aligned
				bytes = new Uint8A(newLength + dictLength);
				bytes.set(dictionary) // copy dictionary into start
				// the section after the dictionary is the target area for get values
				bytes = bytes.subarray(dictLength);
				this.compression.setBuffer(bytes, newLength, dictionary, dictLength);
				bytes.maxLength = newLength;
				Object.defineProperty(bytes, 'length', { value: newLength, writable: true, configurable: true });
				this.compression.getValueBytes = bytes;
			} else {
				console.log('should not get here', newLength)
				bytes = makeReusableBuffer(newLength);
				setGlobalBuffer(getValueBytes = bytes);
			}
			bytes.isGlobal = true;
			return bytes;
		},
		getBinary(id) {
			try {
				asSafeBuffer = true;
				let fastBuffer = this.getBinaryFast(id);
				return fastBuffer && (fastBuffer.isGlobal ? Uint8ArraySlice.call(fastBuffer, 0, this.lastSize) : fastBuffer);
			} finally {
				asSafeBuffer = false;
			}
		},
		getSharedBinary(id) {
			let fastBuffer = this.getBinaryFast(id);
			if (fastBuffer) {
				if (fastBuffer.isGlobal || writeTxn)
					return Uint8ArraySlice.call(fastBuffer, 0, this.lastSize)
				fastBuffer.txn = readTxn;
				readTxn.refCount = (readTxn.refCount || 0) + 1;
				return fastBuffer;
			}
		},
		get(id, options) {
			if (this.decoderCopies) {
				// the decoder copies any data, so we can use the fast binary retrieval that overwrites the same buffer space
				let bytes = this.getBinaryFast(id, options);
				return bytes && (bytes == UNMODIFIED ? UNMODIFIED : this.decoder.decode(bytes));
			}
			if (this.encoding == 'binary')
				return this.getBinary(id, options);
			if (this.decoder) {
				// the decoder potentially uses the data from the buffer in the future and needs a stable buffer
				let bytes = this.getBinary(id);
				return bytes && (bytes == UNMODIFIED ? UNMODIFIED : this.decoder.decode(bytes));
			}

			let result = this.getString(id);
			if (result) {
				if (this.encoding == 'json')
					return JSON.parse(result);
			}
			return result;
		},
		getEntry(id) {
			let value = this.get(id);
			if (value !== undefined) {
				if (this.useVersions)
					return {
						value,
						version: getLastVersion(),
						//size: this.lastSize
					};
				else
					return {
						value,
						//size: this.lastSize
					};
			}
		},
		resetReadTxn() {
			resetReadTxn();
		},
		_commitReadTxn() {
			if (readTxn) {
				readTxn.isCommitted = true
				readTxn.commit();
			}
			lastReadTxnRef = null;
			readTxnRenewed = null;
			readTxn = null;
		},
		ensureReadTxn() {
			if (!env.writeTxn && !readTxnRenewed)
				renewReadTxn(this);
		},
		doesExist(key, versionOrValue) {
			if (!env.writeTxn)
				readTxnRenewed ? readTxn : renewReadTxn(this);
			if (versionOrValue == null) {
				// undefined means the entry exists, null is used specifically to check for the entry *not* existing
				return (this.getBinaryFast(key) === undefined) == (versionOrValue === null);
			}
			else if (this.useVersions) {
				this.getBinaryFast(key);
				return this.getBinaryFast(key) !== undefined && getLastVersion() === versionOrValue;
			}
			else {
				if (versionOrValue && versionOrValue['\x10binary-data\x02'])
					versionOrValue = versionOrValue['\x10binary-data\x02'];
				else if (this.encoder)
					versionOrValue = this.encoder.encode(versionOrValue);
				if (typeof versionOrValue == 'string')
					versionOrValue = Buffer.from(versionOrValue);
				return this.getValuesCount(key, { start: versionOrValue, exactMatch: true}) > 0;
			}
		},
		getValues(key, options) {
			let defaultOptions = {
				key,
				valuesForKey: true
			};
			if (options && options.snapshot === false)
				throw new Error('Can not disable snapshots for getValues');
			return this.getRange(options ? Object.assign(defaultOptions, options) : defaultOptions);
		},
		getKeys(options) {
			if (!options)
				options = {};
			options.values = false;
			return this.getRange(options);
		},
		getCount(options) {
			if (!options)
				options = {};
			options.onlyCount = true;
			return this.getRange(options).iterate();
		},
		getKeysCount(options) {
			if (!options)
				options = {};
			options.onlyCount = true;
			options.values = false;
			return this.getRange(options).iterate();
		},
		getValuesCount(key, options) {
			if (!options)
				options = {};
			options.key = key;
			options.valuesForKey = true;
			options.onlyCount = true;
			return this.getRange(options).iterate();
		},
		getRange(options) {
			let iterable = new RangeIterable();
			if (!options)
				options = {};
			let includeValues = options.values !== false;
			let includeVersions = options.versions;
			let valuesForKey = options.valuesForKey;
			let limit = options.limit;
			let db = this.db;
			let snapshot = options.snapshot;
			let compression = this.compression;
			iterable.iterate = () => {
				let currentKey = valuesForKey ? options.key : options.start;
				const reverse = options.reverse;
				let count = 0;
				let cursor, cursorRenewId, cursorAddress;
				let txn;
				let flags = (includeValues ? 0x100 : 0) | (reverse ? 0x400 : 0) |
					(valuesForKey ? 0x800 : 0) | (options.exactMatch ? 0x4000 : 0) |
					(options.inclusiveEnd ? 0x8000 : 0) |
					(options.exclusiveStart ? 0x10000 : 0);
				let store = this;
				function resetCursor() {
					try {
						if (cursor)
							finishCursor();
						let writeTxn = env.writeTxn;
						if (writeTxn)
							snapshot = false;
						txn = writeTxn || (readTxnRenewed ? readTxn : renewReadTxn(store));
						cursor = !writeTxn && db.availableCursor;
						if (cursor) {
							db.availableCursor = null;
							flags |= 0x2000;
						} else {
							cursor = new Cursor(db);
						}
						cursorAddress = cursor.address;
						txn.refCount = (txn.refCount || 0) + 1; // track transaction so we always use the same one
						if (snapshot === false) {
							cursorRenewId = renewId; // use shared read transaction
							txn.renewingrefCount = (txn.renewingrefCount || 0) + 1; // need to know how many are renewing cursors
						}
					} catch(error) {
						if (cursor) {
							try {
								cursor.close();
							} catch(error) { }
						}
						throw error;
					}
				}
				resetCursor();
				if (options.onlyCount) {
					flags |= 0x1000;
					let count = position(options.offset);
					if (count < 0)
						lmdbError(count);
					finishCursor();
					return count;
				}
				function position(offset) {
					let keySize = currentKey === undefined ? 0 : store.writeKey(currentKey, keyBytes, 0);
					let endAddress;
					if (valuesForKey) {
						if (options.start === undefined && options.end === undefined)
							endAddress = 0;
						else {
							let startAddress;
							if (store.encoder.writeKey) {
								startAddress = saveKey(options.start, store.encoder.writeKey, iterable, maxKeySize);
								keyBytesView.setFloat64(START_ADDRESS_POSITION, startAddress, true);
								endAddress = saveKey(options.end, store.encoder.writeKey, iterable, maxKeySize);
							} else if ((!options.start || options.start instanceof Uint8Array) && (!options.end || options.end instanceof Uint8Array)) {
								startAddress = saveKey(options.start, orderedBinary.writeKey, iterable, maxKeySize);
								keyBytesView.setFloat64(START_ADDRESS_POSITION, startAddress, true);
								endAddress = saveKey(options.end, orderedBinary.writeKey, iterable, maxKeySize);
							} else {
								throw new Error('Only key-based encoding is supported for start/end values');
								let encoded = store.encoder.encode(options.start);
								let bufferAddress = encoded.buffer.address || (encoded.buffer.address = getAddress(encoded) - encoded.byteOffset);
								startAddress = bufferAddress + encoded.byteOffset;
							}
						}
					} else
						endAddress = saveKey(options.end, store.writeKey, iterable, maxKeySize);
					return doPosition(cursorAddress, flags, offset || 0, keySize, endAddress);
				}

				function finishCursor() {
					if (txn.isDone)
						return;
					if (cursorRenewId)
						txn.renewingrefCount--;
					if (--txn.refCount <= 0 && txn.onlyCursor) {
						cursor.close();
						txn.abort(); // this is no longer main read txn, abort it now that we are done
						txn.isDone = true;
					} else {
						if (db.availableCursor || txn != readTxn) {
							cursor.close();
						} else { // try to reuse it
							db.availableCursor = cursor;
							db.cursorTxn = txn;
						}
					}
				}
				return {
					next() {
						let keySize, lastSize;
						if (cursorRenewId && (cursorRenewId != renewId || txn.isDone)) {
							resetCursor();
							keySize = position(0);
						}
						if (count === 0) { // && includeValues) // on first entry, get current value if we need to
							keySize = position(options.offset);
						} else
							keySize = iterate(cursorAddress);
						if (keySize <= 0 ||
								(count++ >= limit)) {
							if (count < 0)
								lmdbError(count);
							finishCursor();
							return ITERATOR_DONE;
						}
						if (!valuesForKey || snapshot === false) {
							if (keySize > 20000) {
								if (keySize > 0x1000000)
									lmdbError(keySize - 0x100000000)
								throw new Error('Invalid key size ' + keySize.toString(16))
							}
							currentKey = store.readKey(keyBytes, 32, keySize + 32);
						}
						if (includeValues) {
							let value;
							lastSize = keyBytesView.getUint32(0, true);
							let bufferId = keyBytesView.getUint32(4, true);
							let bytes;
							if (bufferId) {
								bytes = getMMapBuffer(bufferId, lastSize);
							} else {
								bytes = compression ? compression.getValueBytes : getValueBytes;
								if (lastSize > bytes.maxLength) {
									store.lastSize = lastSize;
									asSafeBuffer = store.encoding == 'binary';
									try {
										bytes = store._returnLargeBuffer(() => getCurrentValue(cursorAddress));
									} finally {
										asSafeBuffer = false;
									}
								} else
									bytes.length = lastSize;
							}
							if (store.decoder) {
								value = store.decoder.decode(bytes, lastSize);
							} else if (store.encoding == 'binary')
								value = bytes.isGlobal ? Uint8ArraySlice.call(bytes, 0, lastSize) : bytes;
							else {
								value = bytes.toString('utf8', 0, lastSize);
								if (store.encoding == 'json' && value)
									value = JSON.parse(value);
							}
							if (includeVersions)
								return {
									value: {
										key: currentKey,
										value,
										version: getLastVersion()
									}
								};
 							else if (valuesForKey)
								return {
									value
								};
							else
								return {
									value: {
										key: currentKey,
										value,
									}
								};
						} else if (includeVersions) {
							return {
								value: {
									key: currentKey,
									version: getLastVersion()
								}
							};
						} else {
							return {
								value: currentKey
							};
						}
					},
					return() {
						finishCursor();
						return ITERATOR_DONE;
					},
					throw() {
						finishCursor();
						return ITERATOR_DONE;
					}
				};
			};
			return iterable;
		},

		getMany(keys, callback) {
			// this is an asynchronous get for multiple keys. It actually works by prefetching asynchronously,
			// allowing a separate to absorb the potentially largest cost: hard page faults (and disk I/O).
			// And then we just do standard sync gets (to deserialized data) to fulfil the callback/promise
			// once the prefetch occurs
			let promise = callback ? undefined : new Promise(resolve => callback = (error, results) => resolve(results));
			this.prefetch(keys, () => {
				let results = new Array(keys.length);
				for (let i = 0, l = keys.length; i < l; i++) {
					results[i] = get.call(this, keys[i]);
				}
				callback(null, results);
			});
			return promise;
		},
		getSharedBufferForGet(id) {
			let txn = (env.writeTxn || (readTxnRenewed ? readTxn : renewReadTxn(this)));
			this.lastSize = this.keyIsCompatibility ? txn.getBinaryShared(id) : this.db.get(this.writeKey(id, keyBytes, 0));
			if (this.lastSize === -30798) { // not found code
				return; //undefined
			}
			return this.lastSize;
			this.lastSize = keyBytesView.getUint32(0, true);
			let bufferIndex = keyBytesView.getUint32(12, true);
			lastOffset = keyBytesView.getUint32(8, true);
			let buffer = buffers[bufferIndex];
			let startOffset;
			if (!buffer || lastOffset < (startOffset = buffer.startOffset) || (lastOffset + this.lastSize > startOffset + 0x100000000)) {
				if (buffer)
					env.detachBuffer(buffer.buffer);
				startOffset = (lastOffset >>> 16) * 0x10000;
				console.log('make buffer for address', bufferIndex * 0x100000000 + startOffset);
				buffer = buffers[bufferIndex] = Buffer.from(getBufferForAddress(bufferIndex * 0x100000000 + startOffset));
				buffer.startOffset = startOffset;
			}
			lastOffset -= startOffset;
			return buffer;
			return buffer.slice(lastOffset, lastOffset + this.lastSize);/*Uint8ArraySlice.call(buffer, lastOffset, lastOffset + this.lastSize)*/
		},
		prefetch(keys, callback) {
			if (!keys)
				throw new Error('An array of keys must be provided');
			if (!keys.length) {
				if (callback) {
					callback(null);
					return;
				} else
					return Promise.resolve();
			}
			let buffers = [];
			let startPosition;
			let bufferHolder = {};
			let lastBuffer;
			for (let key of keys) {
				let position = saveKey(key, this.writeKey, bufferHolder, maxKeySize);
				if (!startPosition)
					startPosition = position;
				if (bufferHolder.saveBuffer != lastBuffer) {
					buffers.push(bufferHolder);
					lastBuffer = bufferHolder.saveBuffer;
					bufferHolder = { saveBuffer: lastBuffer };
				}
			}
			saveKey(undefined, this.writeKey, bufferHolder, maxKeySize);
			prefetch(this.dbAddress, startPosition, (error) => {
				if (error)
					console.error('Error with prefetch', buffers, bufferHolder); // partly exists to keep the buffers pinned in memory
				else
					callback(null);
			});
			if (!callback)
				return new Promise(resolve => callback = resolve);
		},
		useReadTxn() {
			let txn = readTxnRenewed ? readTxn : renewReadTxn(this);
			txn.refCount = (txn.refCount || 0) + 1;
		},
		close(callback) {
			this.status = 'closing';
			if (this.isRoot) {
				if (readTxn) {
					try {
						readTxn.abort();
					} catch(error) {}
				}
				readTxn = {
					renew() {
						throw new Error('Can not read from a closed database');
					}
				};
				readTxnRenewed = null;
			}
			let txnPromise = this._endWrites();
			const doClose = () => {
				if (this.isRoot)
					env.close();
				else
					this.db.close();
				this.status = 'closed';
				if (callback)
					callback();
			}
			if (txnPromise)
				return txnPromise.then(doClose);
			else {
				doClose();
				return Promise.resolve();
			}
		},
		getStats() {
			(env.writeTxn || (readTxnRenewed ? readTxn : renewReadTxn(this)));
			let dbStats = this.db.stat();
			dbStats.root = env.stat();
			dbStats.env = env.info();
			dbStats.free = env.freeStat();
			return dbStats;
		},
	});
	let get = LMDBStore.prototype.get;
	let lastReadTxnRef;
	function getMMapBuffer(bufferId, size) {
		let buffer = mmaps[bufferId];
		if (!buffer) {
			buffer = mmaps[bufferId] = getSharedBuffer(bufferId, env.address);
		}
		let offset = keyBytesView.getUint32(8, true);
		return new Uint8Array(buffer, offset, size);
	}
	function renewReadTxn(store) {
		if (!readTxn) {
			let retries = 0;
			let waitArray;
			do {
				try {
					let lastReadTxn = lastReadTxnRef && lastReadTxnRef.deref();
					readTxn = new Txn(env, 0x20000, lastReadTxn && !lastReadTxn.isDone && lastReadTxn);
					if (readTxn.address == 0) {
						readTxn = lastReadTxn;
						if (readTxn.onlyCursor)
							readTxn.onlyCursor = false;
					}
					break;
				} catch (error) {
					if (error.message.includes('temporarily')) {
						if (!waitArray)
							waitArray = new Int32Array(new SharedArrayBuffer(4), 0, 1);
						Atomics.wait(waitArray, 0, 0, retries * 2);
					} else
						throw error;
				}
			} while (retries++ < 100);
		}
		// we actually don't renew here, we let the renew take place in the next 
		// lmdb native read/call so as to avoid an extra native call
		readTxnRenewed = setTimeout(resetReadTxn, 0);
		store.emit('begin-transaction');
		return readTxn;
	}
	function resetReadTxn(hardReset) {
		renewId++;
		if (readTxnRenewed) {
			readTxnRenewed = null;
			if (readTxn.refCount - (readTxn.renewingrefCount || 0) > 0) {
				readTxn.onlyCursor = true;
				lastReadTxnRef = new WeakRef(readTxn);
				readTxn = null;
			} else
				resetTxn(readTxn.address);
		}
	}
}
export function makeReusableBuffer(size) {
	let bytes = typeof Buffer != 'undefined' ? Buffer.alloc(size) : new Uint8Array(size);
	bytes.maxLength = size;
	Object.defineProperty(bytes, 'length', { value: size, writable: true, configurable: true });
	return bytes;
}

/*Txn.prototype.done = function() {
	this.refCount--;
	if (this.refCount == 0 && this.onlyCursor) {
		this.abort();
		this.isDone = true;
	}
}*/


let readInstructions, uint32Instructions, instructionsDataView = { setFloat64() {}, setUint32() {} }, instructionsAddress;
let savePosition = 8000;
let DYNAMIC_KEY_BUFFER_SIZE = 8192;
function allocateInstructionsBuffer() {
	readInstructions = typeof Buffer != 'undefined' ? Buffer.alloc(DYNAMIC_KEY_BUFFER_SIZE) : new Uint8Array(DYNAMIC_KEY_BUFFER_SIZE);
	uint32Instructions = new Uint32Array(readInstructions.buffer, 0, readInstructions.buffer.byteLength >> 2);
	uint32Instructions[2] = 0xf0000000; // indicates a new read task must be started
	instructionsAddress = readInstructions.buffer.address = getAddress(readInstructions);
	readInstructions.dataView = instructionsDataView = new DataView(readInstructions.buffer, readInstructions.byteOffset, readInstructions.byteLength);
	savePosition = 0;
}
export function recordReadInstruction(txn, dbi, key, writeKey, maxKeySize, callback) {
	if (savePosition > 7800) {
		allocateInstructionsBuffer();
	}
	let start = savePosition;
	let keyPosition = savePosition + 16;
	try {
		savePosition = key === undefined ? keyPosition :
			writeKey(key, readInstructions, keyPosition);
	} catch (error) {
		if (error.name == 'RangeError') {
			if (8180 - start < maxKeySize) {
				allocateInstructionsBuffer(); // try again:
				return recordReadInstruction(key, writeKey, saveTo, maxKeySize);
			}
			throw new Error('Key was too large, max key size is ' + maxKeySize);
		} else
			throw error;
	}
	let length = savePosition - keyPosition;
	if (length > maxKeySize) {
		savePosition = start;
		throw new Error('Key of size ' + length + ' was too large, max key size is ' + maxKeySize);
	}
	uint32Instructions[start >> 2] =  length; // save the length
	savePosition = (savePosition + 12) & 0xfffffc;
	nextResolution.callback = callback;
	nextResolution.uint32 = uint32Instructions;
	nextResolution.position = start >> 2;
	nextResolution = nextResolution.next;
	instructionsDataView.setFloat64(start, txn.address, true);
	if (Atomics.or(uint32Instructions, (start >> 2) + 2, dbi)) {
		return start + instructionsAddress;
	}
	// else we are writing to an active queue, don't have to start a new task
}

function resolveReads(async) {
	let instructionStatus;
	while ((instructionStatus = unreadResolution.uint32[unreadResolution.position + 2]) & 0x1000000) {
		let size = unreadResolution.uint32[unreadResolution.position + 3];
		let reference;
		switch(instructionStatus & 0xf) {
			case 0:
				reference = {
					bufferId: unreadResolution.uint32[unreadResolution.position],
					offset: unreadResolution.uint32[unreadResolution.position + 1],
					size
				};
				break;
			default:
				throw new Error('Unknown read response');
		}
		unreadResolution.callback(reference);
		unreadResolution = unreadResolution.next;
	}
}