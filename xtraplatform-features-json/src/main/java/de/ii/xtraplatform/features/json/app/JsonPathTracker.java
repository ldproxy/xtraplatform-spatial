/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.json.app;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zahnen
 */
// TODO: make XMLPathTracker extension of this //NOPMD ForbiddenContent
public class JsonPathTracker {

  private static final Joiner JOINER = Joiner.on('.').skipNulls();

  private final List<String> localPath;

  public JsonPathTracker() {
    this.localPath = new ArrayList<>(64);
  }

  public void track(int depth) {
    shorten(depth);
  }

  public void track(String localName, int depth) {
    if (depth < 0) {
      return;
    }
    shorten(depth);

    track(localName);
  }

  private void shorten(final int depth) {
    if (depth <= 0) {
      return;
    }
    if (depth <= localPath.size()) {
      localPath.subList(depth - 1, localPath.size()).clear();
    }
  }

  public void track(String localName) {
    localPath.add(localName);
  }

  @Override
  public String toString() {
    if (localPath.isEmpty()) {
      return "";
    }
    return JOINER.join(localPath);
  }

  public List<String> asList() {
    if (localPath.isEmpty()) {
      return ImmutableList.of();
    }
    return ImmutableList.copyOf(localPath);
  }
}
