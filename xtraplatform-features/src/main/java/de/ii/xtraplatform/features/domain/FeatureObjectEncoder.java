/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import de.ii.xtraplatform.features.domain.FeatureEventHandlerGeneric.GenericContext;
import java.util.ArrayList;
import java.util.List;

public abstract class FeatureObjectEncoder<
        T extends PropertyBase<T, FeatureSchema>, U extends FeatureBase<T, FeatureSchema>>
    extends FeatureObjectEncoderBase<FeatureSchema, SchemaMapping, T, U>
    implements FeatureTokenEncoder<ModifiableContext<FeatureSchema, SchemaMapping>> {

  @Override
  public Class<? extends ModifiableContext<FeatureSchema, SchemaMapping>> getContextInterface() {
    return GenericContext.class;
  }

  /**
   * A feature type that concatenates root objects prefixes the properties of each object with its
   * index ("0_name") to keep them apart in the mapping. A feature only ever contains the properties
   * of one of the objects, so the property is looked up by its configured name.
   */
  @Override
  protected List<String> toPropertyPath(FeatureSchema featureSchema, List<String> path) {
    if (featureSchema.getConcat().isEmpty() || path.isEmpty()) {
      return path;
    }

    List<String> propertyPath = new ArrayList<>(path);
    propertyPath.set(0, MappingOperationResolver.cleanConcatPath(path.get(0)));

    return propertyPath;
  }

  @Override
  public ModifiableContext<FeatureSchema, SchemaMapping> createContext() {
    return ModifiableGenericContext.create();
  }
}
