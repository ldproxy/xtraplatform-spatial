/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.SchemaBase.Role;
import de.ii.xtraplatform.services.domain.AuditLog;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureTokenTransformerAudit extends FeatureTokenTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureTokenTransformerAudit.class);
  private final String requestId;
  private final AuditLog auditLog;
  private final Map<String, Object> featureHolder = new LinkedHashMap<>();
  private final List<Map<String, Object>> featureList = new ArrayList<>();
  private final boolean includePropertyValues;
  private final List<String> propertyNames;
  private boolean logAllProperties = false;

  public FeatureTokenTransformerAudit(String requestId, AuditLog auditLog) {
    this.requestId = requestId;
    this.auditLog = auditLog;
    includePropertyValues = auditLog.getIncludePropertyValues(requestId);
    propertyNames = new ArrayList<>();
  }

  @Override
  public void onFeatureStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    featureHolder.clear();
    propertyNames.clear();
    context
        .mappings()
        .get(context.type())
        .getTargetSchema()
        .getAudit()
        .ifPresent(b -> logAllProperties = b);
    super.onFeatureStart(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    Optional<FeatureSchema> prop =
        context.mappings().get(context.type()).getTargetSchema().getProperties().stream()
            .filter(p -> p.getFullPathAsString().equals(context.pathTracker().toString()))
            .findFirst();

    if (prop.isEmpty()) {
      LOGGER.error("No property found with path: {}", context.pathTracker().toString());
      super.onValue(context);
      return;
    }

    FeatureSchema schema = prop.get();
    if (schema.getRole().filter(Role.ID::equals).isPresent()) {
      featureHolder.put("id", context.value());
      super.onValue(context);
      return;
    }

    // Gather necessary information about the property
    String schemaName = schema.getName();
    String value = context.value();
    Optional<Boolean> optAudit = schema.getAudit();

    // Property should explicitly be audited / not audited
    if (optAudit.isPresent()) {
      if (Boolean.TRUE.equals(optAudit.get())) {
        addProperty(schemaName, value);
      }
      super.onValue(context);
      return;
    }

    // Case logAllProperties
    if (logAllProperties) {
      addProperty(schemaName, value);
    }

    super.onValue(context);
  }

  private void addProperty(String schemaName, String value) {
    if (includePropertyValues) {
      featureHolder.put(schemaName, value);
    } else {
      propertyNames.add(schemaName);
    }
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

  @Override
  public void onEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    auditLog.setTarget(requestId, Map.of("features", featureList));
    super.onEnd(context);
  }
}
