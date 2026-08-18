/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

/**
 * Receives progress from and requests cancellation of a running query or mutation. Queries carry
 * the hook as an {@code Optional}; an instance exists only when there is a real hook, an absent
 * hook means no instrumentation.
 *
 * <p>All methods may be called from stream/executor threads; implementations must be thread-safe
 * and {@link #isCancelRequested()} must be cheap enough to poll per feature.
 */
public interface JobHook {

  /** The total the progress reported via {@link #update(int)} counts towards. */
  default void init(int total) {}

  /** Adds {@code delta} to the current progress. */
  default void update(int delta) {}

  default boolean isCancelRequested() {
    return false;
  }

  /** Cooperative cancellation point. */
  default void checkpoint() {
    if (isCancelRequested()) {
      throw new JobCancelledException();
    }
  }
}
