/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class FeatureTokenTransformerRemoveEmptyOptionals extends FeatureTokenTransformer {

  private final List<String> nestingStack;
  private final List<FeatureSchema> schemaStack;
  private final Map<String, Boolean> removeNullValues;

  public FeatureTokenTransformerRemoveEmptyOptionals(
      Map<String, PropertyTransformations> propertyTransformations) {
    this.nestingStack = new ArrayList<>();
    this.schemaStack = new ArrayList<>();
    this.removeNullValues =
        propertyTransformations.keySet().stream()
            .map(
                type ->
                    Map.entry(
                        type,
                        !propertyTransformations
                            .get(type)
                            .hasTransformation(
                                PropertyTransformations.WILDCARD,
                                pt ->
                                    pt.getRemoveNullValues().isPresent()
                                        && pt.getRemoveNullValues().get() == false)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public void onObjectStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (context.schema().isEmpty()) {
      return;
    }

    FeatureSchema schema = context.schema().get();

    if (schema.isRequired() && schemaStack.isEmpty()) {
      getDownstream().onObjectStart(context);
    } else {
      nestingStack.add("O");
      schemaStack.add(schema);
    }
  }

  @Override
  public void onObjectEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (context.schema().isEmpty()) {
      return;
    }

    if (schemaStack.isEmpty()) {
      getDownstream().onObjectEnd(context);
    } else {
      nestingStack.remove(nestingStack.size() - 1);
      schemaStack.remove(schemaStack.size() - 1);
    }
  }

  @Override
  public void onArrayStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (context.schema().isEmpty()) {
      return;
    }

    FeatureSchema schema = context.schema().get();

    if (schema.isRequired() && schemaStack.isEmpty()) {
      getDownstream().onArrayStart(context);
    } else {
      nestingStack.add("A");
      schemaStack.add(schema);
    }
  }

  @Override
  public void onArrayEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (context.schema().isEmpty()) {
      return;
    }

    if (schemaStack.isEmpty()) {
      getDownstream().onArrayEnd(context);
    } else {
      nestingStack.remove(nestingStack.size() - 1);
      schemaStack.remove(schemaStack.size() - 1);
    }
  }

  @Override
  public void onGeometry(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (context.schema().isEmpty()) {
      return;
    }

    if (Objects.nonNull(context.geometry())
        || context.schema().get().isRequired()
        || !removeNullValues.getOrDefault(context.type(), true)) {
      openIfNecessary(context);

      super.onGeometry(context);
    }
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (context.schema().isEmpty()) {
      return;
    }

    if (Objects.nonNull(context.value())
        || context.schema().get().isRequired()
        || !removeNullValues.getOrDefault(context.type(), true)) {
      openIfNecessary(context);

      super.onValue(context);
    }
  }

  private void openIfNecessary(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (!schemaStack.isEmpty()) {
      List<String> previousPath = context.path();
      for (int i = 0; i < schemaStack.size(); i++) {
        FeatureSchema schema = schemaStack.get(i);
        context.pathTracker().track(schema.getFullPath());

        if (nestingStack.get(i).equals("A")) {
          getDownstream().onArrayStart(context);
          context.setInArray(true);
        } else if (nestingStack.get(i).equals("O")) {
          getDownstream().onObjectStart(context);
          context.setInObject(true);
        }
      }
      nestingStack.clear();
      schemaStack.clear();
      context.pathTracker().track(previousPath);
    }
  }
}
