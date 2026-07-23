/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import java.util.List;

/**
 * Thrown by {@link FeatureTransactions.Session#execute(List)} when one of the raw statements (a
 * transaction-lifecycle hook) fails. This is an expected, configuration-driven outcome — it
 * triggers a rollback and is reported to the client — not an illegal state, so callers should log
 * it quietly. Any non-fatal warnings collected from statements that ran before the failing one are
 * carried in {@link #getWarnings()} so they are not lost on the failure path.
 */
public class FeatureMutationHookException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final List<String> warnings;

  public FeatureMutationHookException(String message, Throwable cause, List<String> warnings) {
    super(message, cause);
    this.warnings = List.copyOf(warnings);
  }

  /** Warnings emitted by hook statements that ran successfully before the failing statement. */
  public List<String> getWarnings() {
    return warnings;
  }
}
