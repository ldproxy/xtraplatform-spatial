/*
 * Copyright 2017 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.xml;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zahnen
 */
public class XMLPathTracker {
  private final List<String> localPath;
  private final List<String> path;
  private final List<String> noObjectPath;
  private final Joiner joiner;
  private final Joiner dotJoiner;

  private boolean multiple;

  public XMLPathTracker() {
    this.multiple = false;
    this.localPath = new ArrayList<>();
    this.path = new ArrayList<>();
    this.noObjectPath = new ArrayList<>();
    this.joiner = Joiner.on('/').skipNulls();
    this.dotJoiner = Joiner.on('.').skipNulls();
  }

  public void track(int depth) {
    shorten(depth);
  }

  public void track(String nsuri, String localName, int depth, boolean isMultiple) {
    if (depth < 0) {
      return;
    }
    shorten(depth);

    track(nsuri, localName, isMultiple);
  }

  private void shorten(final int depth) {
    if (depth <= 0) {
      return;
    }
    if (depth <= localPath.size()) {
      localPath.subList(depth - 1, localPath.size()).clear();
    }
    if (depth <= path.size()) {
      path.subList(depth - 1, path.size()).clear();
      noObjectPath.subList(depth - 1, noObjectPath.size()).clear();
    }
  }

  public void track(String nsuri, String localName, boolean isMultiple) {
    localPath.add(localName);
    if (nsuri != null && localName != null) {
      path.add(nsuri + ":" + localName);
    }
    if (localName != null
        && (!Character.isUpperCase(localName.charAt(0)) || noObjectPath.isEmpty())) {
      noObjectPath.add(localName + (isMultiple ? "[]" : ""));
    } else {
      noObjectPath.add(null);
    }
  }

  public boolean isMultiple() {
    return multiple;
  }

  public void setMultiple(boolean multiple) {
    this.multiple = multiple;
  }

  public String toFieldName() {
    return joiner.join(localPath);
  }

  public String toFieldNameGml() {
    return dotJoiner.join(noObjectPath);
  }

  @Override
  public String toString() {
    return joiner.join(path);
  }

  public String toLocalPath() {
    return joiner.join(localPath);
  }

  public List<String> asList() {
    return path;
  }

  public boolean isEmpty() {
    return path.isEmpty();
  }

  public void clear() {
    localPath.clear();
    path.clear();
    this.multiple = false;
  }
}
