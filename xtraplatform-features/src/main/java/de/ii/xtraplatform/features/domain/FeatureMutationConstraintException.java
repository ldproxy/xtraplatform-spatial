/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

/**
 * Thrown when a mutation statement is rejected by the database because it violates an integrity
 * constraint — a CHECK or foreign-key constraint, a unique index, or a trigger raising an error in
 * SQLSTATE class 23. This is caused by the data the client sent, not by a bug or an infrastructure
 * problem: it is reported to the client and rolls the transaction back, so callers should log it
 * quietly, without a stack trace.
 */
public class FeatureMutationConstraintException extends RuntimeException {

  private final String sqlState;

  public FeatureMutationConstraintException(String message, Throwable cause, String sqlState) {
    super(message, cause);
    this.sqlState = sqlState;
  }

  /** The SQLSTATE reported by the database, always in class 23. */
  public String getSqlState() {
    return sqlState;
  }
}
