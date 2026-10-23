/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.xtralink.domain.JobProcessing;

public abstract class JobHookCancellable implements JobHook {

  protected final JobProcessing jobs;
  protected final String jobId;
  private boolean cancelRequested;

  public JobHookCancellable(JobProcessing jobs, String jobId) {
    this.jobs = jobs;
    this.jobId = jobId;
    this.cancelRequested = false;
  }

  @Override
  public final boolean isCancelRequested() {
    if (cancelRequested) {
      return true;
    }
    if (jobs.isCancelled(jobId)) {
      this.cancelRequested = true;
    }
    return cancelRequested;
  }
}
