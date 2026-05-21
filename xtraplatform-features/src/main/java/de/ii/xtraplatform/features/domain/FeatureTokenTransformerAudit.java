/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.ii.xtraplatform.base.domain.AuditLog;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureTokenTransformerAudit extends FeatureTokenTransformer {

  private final String requestId;
  private final AuditLog auditLog;

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureTokenTransformerAudit.class);

  public FeatureTokenTransformerAudit(
      ImmutableResult.Builder resultBuilder, String requestId, AuditLog auditLog) {
    this.requestId = requestId;
    this.auditLog = auditLog;
  }

  public <X> FeatureTokenTransformerAudit(
      ImmutableResultReduced.Builder<X> resultBuilder, String requestId, AuditLog auditLog) {
    this.requestId = requestId;
    this.auditLog = auditLog;
  }

  @Override
  public void onStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    for (String type : context.mappings().keySet()) {
      for (FeatureSchema prop : context.mappings().get(type).getTargetSchema().getProperties()) {
        if (prop.getAudit().isPresent()) {
          String audit = prop.getAudit().get().toLowerCase();
          switch (audit) {
            case "access":
              auditLog.initPropertyToAccessTrack(requestId, type, prop.getName());
              continue;
            case "value":
              auditLog.initPropertyToValueTrack(requestId, type, prop.getName());
              continue;
            case "none":
              continue;
            default:
              LOGGER.error("Wrong entry for audit: {}", audit);
          }
        }
      }
    }

    super.onStart(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    String type = context.type();
    Optional<FeatureSchema> prop =
        context.mappings().get(type).getTargetSchema().getProperties().stream()
            .filter(p -> p.getFullPathAsString().equals(context.pathTracker().toString()))
            .findFirst();

    List<FeatureSchema> test = context.mappings().get(type).getTargetSchema().getProperties();
    List<FeatureSchema> testid = context.mappings().get(type).getTargetSchema().getIdProperties();
    FeatureSchema testtarget = context.mappings().get(type).getTargetSchema();
    String testname = context.mappings().get(type).getTargetSchema().getName();
    if (prop.isPresent()) {
      if (prop.get().getAudit().isPresent()) {
        String audit = prop.get().getAudit().get().toLowerCase();
        switch (audit) {
          case "access":
            auditLog.markPropertyAccessed(requestId, type, prop.get().getName());
            break;
          case "value":
            auditLog.appendPropertyValue(requestId, type, prop.get().getName(), context.value());
            break;
          case "none":
            break;
          default:
            LOGGER.error("Wrong entry for audit: {}", audit);
        }
      }
    } else {
      LOGGER.error("No property found with path: {}", context.pathTracker().toString());
    }

    super.onValue(context);
  }

  @Override
  public void onEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    try {
      auditLog.saveLogToFileAndRemove(requestId);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    super.onEnd(context);
  }
}
