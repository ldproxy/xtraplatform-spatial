/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.transform.PropertyTransformation;
import java.util.List;
import java.util.Optional;

public interface FeatureSchemaBase<T extends FeatureSchemaBase<T>> extends SchemaBase<T> {

  Optional<String> getObjectType();

  Optional<String> getOriginObjectType();

  Optional<String> getUnit();

  Optional<String> getConstantValue();

  List<PropertyTransformation> getTransformations();
}
