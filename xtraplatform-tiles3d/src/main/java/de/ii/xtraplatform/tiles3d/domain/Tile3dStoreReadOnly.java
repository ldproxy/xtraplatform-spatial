/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import de.ii.xtraplatform.blobs.domain.Blob;
import java.io.IOException;
import java.util.Optional;

public interface Tile3dStoreReadOnly {

  boolean has(Tile3dQuery tile) throws IOException;

  Optional<Blob> get(Tile3dQuery tile) throws IOException;

  Optional<Boolean> isEmpty(Tile3dQuery tile) throws IOException;

  boolean isEmpty() throws IOException;
}
