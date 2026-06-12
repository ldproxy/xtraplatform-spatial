/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.services.domain.AuditLog;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FeatureTokenTransformerAudit extends FeatureTokenTransformer {

  private final String requestId;
  private final AuditLog auditLog;
  private final Map<String, Object> featureHolder;
  private final List<Map<String, Object>> featureList;
  private final boolean includePropertyValues;
  private final List<String> propertyNames;
  private boolean logAllProperties;

  public FeatureTokenTransformerAudit(String requestId, AuditLog auditLog) {
    this.requestId = requestId;
    this.auditLog = auditLog;
    this.includePropertyValues = auditLog.getIncludePropertyValues(requestId);
    this.propertyNames = new ArrayList<>();
    this.featureHolder = new LinkedHashMap<>();
    this.featureList = new ArrayList<>();
    this.logAllProperties = false;
  }

  @Override
  public void onFeatureStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    featureHolder.clear();
    propertyNames.clear();

    this.logAllProperties = context.schema().flatMap(FeatureSchema::getAudit).orElse(false);

    super.onFeatureStart(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    Optional<FeatureSchema> prop = context.schema();

    if (prop.isEmpty()) {
      super.onValue(context);
      return;
    }

    FeatureSchema schema = prop.get();
    if (schema.isId()) {
      featureHolder.put("id", context.value());
      super.onValue(context);
      return;
    }

    boolean doAudit =
        (logAllProperties && schema.getAudit().orElse(true)) || schema.getAudit().orElse(false);
    if (doAudit) {
      String schemaName = schema.getFullPathAsString();
      String value = context.value();

      addProperty(schemaName, value, context.inArray());
    }

    super.onValue(context);
  }

  private void addProperty(String schemaName, String value, boolean isMultiValue) {
    if (!includePropertyValues) {
      propertyNames.add(schemaName);
      return;
    }

    if (!isMultiValue) {
      featureHolder.put(schemaName, value);
      return;
    }

    List<String> values =
        (List<String>) featureHolder.computeIfAbsent(schemaName, k -> new ArrayList<String>());

    values.add(value);
  }

  @Override
  public void onFeatureEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (!includePropertyValues) {
      featureHolder.put("properties", new ArrayList<>(propertyNames));
    }
    featureList.add(new LinkedHashMap<>(featureHolder));
    logAllProperties = false;

    super.onFeatureEnd(context);
  }

  public void appendToLog() {
    auditLog.setTarget(requestId, Map.of("features", featureList));
  }
}
