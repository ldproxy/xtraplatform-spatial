/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;

/**
 * A memory-efficient set of feature ids. Ids are not stored as strings: an id with up to 20
 * characters from {@code [0-9A-Za-z_-]} is packed losslessly into 128 bits, any other id is
 * represented by its 128-bit hash (collisions are negligible). The set is an open-addressing hash
 * table of long pairs, so the memory footprint is 32 bytes per id at the default load factor.
 *
 * <p>The number of entries is bounded; adding an id beyond the bound throws an {@link
 * IllegalStateException}.
 */
public class PackedIdSet {

  private static final long PACKED_MARKER = 0x8000_0000_0000_0000L;
  private static final int MAX_PACKED_LENGTH = 20;
  private static final int INITIAL_CAPACITY = 1024;

  private final int maxEntries;
  private long[] table;
  private int capacity;
  private int size;
  private boolean containsZero;

  public PackedIdSet(int maxEntries) {
    this.maxEntries = maxEntries;
    this.capacity = INITIAL_CAPACITY;
    this.table = new long[2 * capacity];
    this.size = 0;
    this.containsZero = false;
  }

  /**
   * Adds an id to the set.
   *
   * @param id the feature id
   * @return {@code true} if the id was not in the set
   */
  public boolean add(String id) {
    long hi;
    long lo;

    long[] packed = pack(id);
    if (packed.length > 0) {
      hi = packed[0];
      lo = packed[1];
    } else {
      HashCode hash = Hashing.murmur3_128().hashString(id, StandardCharsets.UTF_8);
      byte[] bytes = hash.asBytes();
      hi = toLong(bytes, 0) & ~PACKED_MARKER;
      lo = toLong(bytes, 8);
    }

    if (hi == 0 && lo == 0) {
      if (containsZero) {
        return false;
      }
      checkBound();
      this.containsZero = true;
      this.size++;
      return true;
    }

    int index = findSlot(table, capacity, hi, lo);
    if (table[index] == hi && table[index + 1] == lo) {
      return false;
    }

    checkBound();
    table[index] = hi;
    table[index + 1] = lo;
    this.size++;

    if (size > capacity / 2 && capacity < Integer.MAX_VALUE / 4) {
      grow();
    }

    return true;
  }

  public int size() {
    return size;
  }

  private void checkBound() {
    if (size >= maxEntries) {
      throw new IllegalStateException(
          String.format(
              "The response exceeds the maximum number of features that can be deduplicated (%d).",
              maxEntries));
    }
  }

  private void grow() {
    int newCapacity = capacity * 2;
    long[] newTable = new long[2 * newCapacity];

    for (int i = 0; i < table.length; i += 2) {
      long hi = table[i];
      long lo = table[i + 1];
      if (hi != 0 || lo != 0) {
        int index = findSlot(newTable, newCapacity, hi, lo);
        newTable[index] = hi;
        newTable[index + 1] = lo;
      }
    }

    this.table = newTable;
    this.capacity = newCapacity;
  }

  private static int findSlot(long[] table, int capacity, long hi, long lo) {
    int mask = capacity - 1;
    int slot = spread(hi, lo) & mask;

    while (true) {
      int index = 2 * slot;
      if ((table[index] == 0 && table[index + 1] == 0)
          || (table[index] == hi && table[index + 1] == lo)) {
        return index;
      }
      slot = (slot + 1) & mask;
    }
  }

  private static int spread(long hi, long lo) {
    long mixed = (hi ^ (hi >>> 32)) * 0x9E3779B97F4A7C15L + (lo ^ (lo >>> 32));
    return (int) (mixed ^ (mixed >>> 32)) & Integer.MAX_VALUE;
  }

  // 6 bits per character plus the length, so that ids with leading zero-characters stay distinct
  private static long[] pack(String id) {
    int length = id.length();
    if (length == 0 || length > MAX_PACKED_LENGTH) {
      return new long[0];
    }

    long hi = 0;
    long lo = 0;
    for (int i = 0; i < length; i++) {
      int bits = toBits(id.charAt(i));
      if (bits < 0) {
        return new long[0];
      }
      hi = (hi << 6) | (lo >>> 58);
      lo = (lo << 6) | bits;
    }
    hi |= ((long) length) << 56;
    hi |= PACKED_MARKER;

    return new long[] {hi, lo};
  }

  private static int toBits(char c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    }
    if (c >= 'A' && c <= 'Z') {
      return c - 'A' + 10;
    }
    if (c >= 'a' && c <= 'z') {
      return c - 'a' + 36;
    }
    if (c == '-') {
      return 62;
    }
    if (c == '_') {
      return 63;
    }
    return -1;
  }

  private static long toLong(byte[] bytes, int offset) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result = (result << 8) | (bytes[offset + i] & 0xFF);
    }
    return result;
  }
}
