/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transcode;

import java.io.IOException;

public interface SeperateStringsProcessor {

  void onStart() throws IOException;

  void onSeparator() throws IOException;

  void onX(char[] chars, int offset, int length) throws IOException;

  void onY(char[] chars, int offset, int length) throws IOException;

  void onZ(char[] chars, int offset, int length) throws IOException;

  default void onFlush() throws IOException {}

  void onEnd() throws IOException;
}
