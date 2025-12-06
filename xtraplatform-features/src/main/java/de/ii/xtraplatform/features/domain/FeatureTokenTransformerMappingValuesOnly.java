/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.codelists.domain.Codelist;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.transform.FeaturePropertyValueTransformer;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import de.ii.xtraplatform.features.domain.transform.TransformerChain;
import java.time.ZoneId;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureTokenTransformerMappingValuesOnly extends FeatureTokenTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FeatureTokenTransformerMappingValuesOnly.class);

  private final Map<String, PropertyTransformations> propertyTransformations;
  private final Map<String, Codelist> codelists;
  private final ZoneId nativeTimeZone;
  private Map<String, TransformerChain<String, FeaturePropertyValueTransformer>>
      valueTransformerChains;
  private TransformerChain<String, FeaturePropertyValueTransformer> currentValueTransformerChain;

  public FeatureTokenTransformerMappingValuesOnly(
      Map<String, PropertyTransformations> propertyTransformations,
      Map<String, Codelist> codelists,
      ZoneId nativeTimeZone) {
    this.propertyTransformations = propertyTransformations;
    this.codelists = codelists;
    this.nativeTimeZone = nativeTimeZone;
  }

  @Override
  protected void init() {
    super.init();
  }

  @Override
  public void onStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    this.valueTransformerChains =
        context.mappings().entrySet().stream()
            .map(
                entry ->
                    new SimpleImmutableEntry<>(
                        entry.getKey(),
                        propertyTransformations
                            .get(entry.getKey())
                            .getValueTransformations(entry.getValue(), codelists, nativeTimeZone)))
            .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

    getDownstream().onStart(context);
  }

  @Override
  public void onFeatureStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    this.currentValueTransformerChain = valueTransformerChains.get(context.type());

    getDownstream().onFeatureStart(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (context.schema().filter(FeatureSchema::isValue).isPresent()) {
      FeatureSchema schema = context.schema().get();
      String value = context.value();

      if (Objects.nonNull(value)) {
        value = currentValueTransformerChain.transform(schema.getFullPathAsString(), value);
        context.setValue(value);

        Type valueType =
            schema.isSpatial()
                ? context.valueType()
                : schema.getValueType().orElse(schema.getType());
        context.setValueType(valueType);
      }

      getDownstream().onValue(context);
    }
  }
}
