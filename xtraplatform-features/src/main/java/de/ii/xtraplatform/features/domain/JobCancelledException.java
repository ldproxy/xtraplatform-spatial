/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

/**
 * Thrown by {@link JobHook#checkpoint()} when cancellation was requested. Inside a token pipeline
 * it rides the regular stream-error path, which cancels the upstream subscription and closes the
 * database resources.
 */
public class JobCancelledException extends RuntimeException {

  public JobCancelledException() {
    super("The request was cancelled");
  }
}
