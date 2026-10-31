/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(deepImmutablesDetection = true)
public interface FeatureStoreTypeInfo {

  // TODO: multiple main containers, sorted, important for e.g. limit // NOPMD
  // (Instance(s|Container))
  // main container must not have oid, is that relevant for read or only for write?
  // main container is itself an attribute container and has 0-n child attribute container
  // (Attribute(s|Container))
  // attribute container implements Relation|Connection with join conditions

  String getName();

  List<FeatureStoreInstanceContainer> getInstanceContainers();
}
