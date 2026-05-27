/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.base.domain.AuditLog;
import de.ii.xtraplatform.features.domain.SchemaBase.Role;
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

  public FeatureTokenTransformerAudit(String requestId, AuditLog auditLog) {
    this.requestId = requestId;
    this.auditLog = auditLog;
  }

  @Override
  public void onFeatureStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    featureHolder.clear();
    super.onFeatureStart(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    String type = context.type();
    Optional<FeatureSchema> prop =
        context.mappings().get(type).getTargetSchema().getProperties().stream()
            .filter(p -> p.getFullPathAsString().equals(context.pathTracker().toString()))
            .findFirst();

    if (prop.isEmpty()) {
      LOGGER.error("No property found with path: {}", context.pathTracker().toString());
      super.onValue(context);
      return;
    }

    if (prop.get().getRole().filter(Role.ID::equals).isPresent()) {
      featureHolder.put("id", context.value());
    } else if (prop.get().getAudit().isPresent()) {
      featureHolder.put(prop.get().getName(), context.value());
    }
    super.onValue(context);
  }

  @Override
  public void onFeatureEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    featureList.add(new LinkedHashMap<>(featureHolder));
    super.onFeatureEnd(context);
  }

  @Override
  public void onEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    auditLog.initTarget(requestId, Map.of("features", featureList));
    super.onEnd(context);
  }
}
