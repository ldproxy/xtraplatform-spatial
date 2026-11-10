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
    super();
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
                                        && !pt.getRemoveNullValues().get())))
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
        || isEffectivelyRequired(context)
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
        || isEffectivelyRequired(context)
        || !removeNullValues.getOrDefault(context.type(), true)) {
      openIfNecessary(context);

      super.onValue(context);
    }
  }

  /**
   * A null value is kept only if its property is effectively required, i.e. the property itself and
   * every wrapping property up to (but excluding) the feature are required. A required property
   * inside an optional object must not keep that object alive when all of its values are null: in
   * that case the optional wrapper, together with the null value, is removed. Without this check
   * the wrapping property is only ever looked at indirectly, so an optional object whose required
   * sub-properties are all null is emitted as an empty object instead of being omitted.
   */
  private boolean isEffectivelyRequired(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (context.schema().isEmpty() || !context.schema().get().isRequired()) {
      return false;
    }

    List<FeatureSchema> parentSchemas = context.parentSchemas();

    return parentSchemas.size() <= 1
        || parentSchemas.stream()
            .limit(parentSchemas.size() - 1L)
            .allMatch(FeatureSchema::isRequired);
  }

  private void openIfNecessary(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (!schemaStack.isEmpty()) {
      List<String> previousPath = context.path();
      for (int i = 0; i < schemaStack.size(); i++) {
        FeatureSchema schema = schemaStack.get(i);
        context.pathTracker().track(schema.getFullPath());

        if ("A".equals(nestingStack.get(i))) {
          getDownstream().onArrayStart(context);
          context.setInArray(true);
        } else if ("O".equals(nestingStack.get(i))) {
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
