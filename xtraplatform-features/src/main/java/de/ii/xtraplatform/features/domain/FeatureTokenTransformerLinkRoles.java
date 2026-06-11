/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Strips property values whose schema role declares a {@link SchemaBase.Role#getLinkRelation() link
 * relation} (e.g. {@code PREDECESSOR_INTERVAL_START}, {@code SUCCESSOR_INTERVAL_START}) from the
 * token stream so downstream encoders do not emit them as inline feature properties. This applies
 * to every response that streams features of a versioned type — single feature, items, search.
 *
 * <p>The captured {@code (linkRelation, value)} pairs are surfaced twice:
 *
 * <ul>
 *   <li>Per feature, via {@link ModifiableContext#setRoleLinks(java.util.Map)}, on every feature in
 *       any response, so format writers can emit per-feature link items in the response body.
 *   <li>Per result, via {@code getRoleLinks()} on {@link FeatureStream.Result} / {@link
 *       FeatureStream.ResultReduced}, which drives the response's HTTP {@code Link} headers. This
 *       is only meaningful for the single-feature endpoint, where the response carries exactly one
 *       feature version (the {@code datetime} parameter resolves to a single instant), so the links
 *       of the only feature in the stream are the links of the response.
 * </ul>
 */
public class FeatureTokenTransformerLinkRoles extends FeatureTokenTransformer {

  private static final DateTimeFormatter FLEXIBLE_PARSER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd[['T'][' ']HH:mm:ss[.SSS]][X]");

  private final Consumer<Map<String, String>> roleLinksSetter;
  private Map<String, String> current = new LinkedHashMap<>();
  private boolean isSingleFeature = false;
  private Map<String, String> resultRoleLinks;

  public FeatureTokenTransformerLinkRoles(ImmutableResult.Builder resultBuilder) {
    this.roleLinksSetter = resultBuilder::roleLinks;
  }

  public <X> FeatureTokenTransformerLinkRoles(ImmutableResultReduced.Builder<X> resultBuilder) {
    this.roleLinksSetter = resultBuilder::roleLinks;
  }

  @Override
  public void onStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    this.isSingleFeature = context.metadata().isSingleFeature();
    super.onStart(context);
  }

  @Override
  public void onFeatureStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    this.current = new LinkedHashMap<>();
    context.setRoleLinks(Map.of());
    super.onFeatureStart(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    Optional<String> linkRelation =
        context.schema().flatMap(SchemaBase::getRole).flatMap(SchemaBase.Role::getLinkRelation);
    if (linkRelation.isPresent() && Objects.nonNull(context.value())) {
      current.put(linkRelation.get(), normalizeToIso(context.value()));
      return;
    }
    super.onValue(context);
  }

  static String normalizeToIso(String value) {
    try {
      TemporalAccessor ta =
          FLEXIBLE_PARSER.parseBest(
              value, OffsetDateTime::from, LocalDateTime::from, LocalDate::from);
      OffsetDateTime odt;
      if (ta instanceof OffsetDateTime) {
        odt = ((OffsetDateTime) ta).withOffsetSameInstant(ZoneOffset.UTC);
      } else if (ta instanceof LocalDateTime) {
        odt = ((LocalDateTime) ta).atZone(ZoneId.of("UTC")).toOffsetDateTime();
      } else {
        odt = ((LocalDate) ta).atStartOfDay(ZoneId.of("UTC")).toOffsetDateTime();
      }
      return DateTimeFormatter.ISO_INSTANT.format(odt.toInstant());
    } catch (Throwable ignore) {
      return value;
    }
  }

  @Override
  public void onFeatureEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (!current.isEmpty()) {
      context.setRoleLinks(current);
      this.resultRoleLinks = current;
    }
    super.onFeatureEnd(context);
  }

  @Override
  public void onEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (isSingleFeature && Objects.nonNull(resultRoleLinks)) {
      roleLinksSetter.accept(resultRoleLinks);
    }
    super.onEnd(context);
  }
}
