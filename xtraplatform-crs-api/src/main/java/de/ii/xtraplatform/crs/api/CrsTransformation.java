/**
 * Copyright 2019 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
/**
 * bla
 */
package de.ii.xtraplatform.crs.api;

import java.util.Optional;

/**
 *
 * @author zahnen
 */
public interface CrsTransformation {

    Optional<CrsTransformer> getTransformer(EpsgCrs sourceCrs, EpsgCrs targetCrs);

    boolean isCrsAxisOrderEastNorth(String crs);

    boolean isCrsSupported(String crs);
    
}
