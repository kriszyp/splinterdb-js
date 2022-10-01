// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * splinterdb_private.h --
 *
 * This file contains the private interfaces implemented in splinterdb.c.
 * These definitions are provided here so that they can be shared by the
 * source and test modules.
 */
#ifndef __SPLINTERDB_PRIVATE_H__
#define __SPLINTERDB_PRIVATE_H__

#include "splinterdb/splinterdb.h"
#include "data_internal.h"

/*
 *-----------------------------------------------------------------------------
 * _splinterdb_lookup_result structure --
 *-----------------------------------------------------------------------------
 */
typedef struct {
   merge_accumulator value;
} _splinterdb_lookup_result;

_Static_assert(sizeof(_splinterdb_lookup_result)
                  <= sizeof(splinterdb_lookup_result),
               "sizeof(splinterdb_lookup_result) is too small");

_Static_assert(alignof(splinterdb_lookup_result)
                  == alignof(_splinterdb_lookup_result),
               "mismatched alignment for splinterdb_lookup_result");

int
splinterdb_create_or_open(const splinterdb_config *kvs_cfg,      // IN
                          splinterdb             **kvs_out,      // OUT
                          bool                     open_existing // IN
);

bool
validate_key_in_range(const splinterdb *kvs, slice key);

#endif // __SPLINTERDB_PRIVATE_H__
