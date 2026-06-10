/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import java.time.Instant;
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
 * token stream so downstream encoders do not emit them as inline feature properties. The captured
 * {@code (linkRelation, value)} pairs are exposed on the {@link FeatureStream.Result} / {@link
 * FeatureStream.ResultReduced} via {@code getRoleLinks()} so the response handler can build HTTP
 * {@code Link} headers and per-feature link items in the response body.
 *
 * <p>Per-feature role values are written to {@link ModifiableContext#setRoleLinks(java.util.Map)}
 * on every feature so writers can emit per-feature link items.
 *
 * <p>The result-level map drives the response's HTTP {@code Link} headers and is only meaningful
 * for the single-feature endpoint. Within a single-feature response that streams several versions,
 * the feature with the greatest {@code PRIMARY_INTERVAL_START} wins — that is the latest version,
 * and its predecessor/successor links describe the navigation that applies to the response as a
 * whole. For non-versioned single-feature responses (no {@code PRIMARY_INTERVAL_START} on any
 * feature) the only feature in the stream wins.
 */
public class FeatureTokenTransformerLinkRoles extends FeatureTokenTransformer {

  private static final DateTimeFormatter FLEXIBLE_PARSER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd[['T'][' ']HH:mm:ss[.SSS]][X]");

  private final Consumer<Map<String, String>> roleLinksSetter;
  private Map<String, String> current = new LinkedHashMap<>();
  private boolean isSingleFeature = false;
  private Instant currentStart;
  private Instant latestStart;
  private Map<String, String> latestRoleLinks;

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
    this.currentStart = null;
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
    if (Objects.nonNull(context.value())
        && context.schema().filter(SchemaBase::isPrimaryIntervalStart).isPresent()) {
      currentStart = parseToInstant(context.value());
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

  static Instant parseToInstant(String value) {
    try {
      TemporalAccessor ta =
          FLEXIBLE_PARSER.parseBest(
              value, OffsetDateTime::from, LocalDateTime::from, LocalDate::from);
      if (ta instanceof OffsetDateTime) {
        return ((OffsetDateTime) ta).toInstant();
      } else if (ta instanceof LocalDateTime) {
        return ((LocalDateTime) ta).atZone(ZoneId.of("UTC")).toInstant();
      } else {
        return ((LocalDate) ta).atStartOfDay(ZoneId.of("UTC")).toInstant();
      }
    } catch (Throwable ignore) {
      return null;
    }
  }

  @Override
  public void onFeatureEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (!current.isEmpty()) {
      context.setRoleLinks(current);
      // Pick the feature with the greatest PRIMARY_INTERVAL_START for the result-level map.
      // For non-versioned single-feature responses there is no start, so the only feature wins.
      if (currentStart != null) {
        if (latestStart == null || currentStart.isAfter(latestStart)) {
          latestStart = currentStart;
          latestRoleLinks = current;
        }
      } else if (latestStart == null) {
        latestRoleLinks = current;
      }
    }
    super.onFeatureEnd(context);
  }

  @Override
  public void onEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (isSingleFeature && Objects.nonNull(latestRoleLinks)) {
      roleLinksSetter.accept(latestRoleLinks);
    }
    super.onEnd(context);
  }
}
