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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Strips property values whose schema declares an {@link FeatureSchema#getEffectiveLink() effective
 * link} — an explicit {@link SchemaLink} or a {@link SchemaBase.Role role} with a link relation
 * (e.g. {@code PREDECESSOR_INTERVAL_START}, {@code SUCCESSOR_INTERVAL_START}) — from the token
 * stream so downstream encoders do not emit them as inline feature properties. This applies to
 * every response that streams features of a type with such properties — single feature, items,
 * search.
 *
 * <p>The captured {@link PropertyLink}s are surfaced twice:
 *
 * <ul>
 *   <li>Per feature, via {@link ModifiableContext#setPropertyLinks(Iterable)}, on every feature in
 *       any response, so format writers can emit per-feature link items in the response body.
 *   <li>Per result, via {@code getPropertyLinks()} on {@link FeatureStream.Result} / {@link
 *       FeatureStream.ResultReduced}, which drives the response's HTTP {@code Link} headers. This
 *       is only meaningful for the single-feature endpoint, where the response carries exactly one
 *       feature, so the links of the only feature in the stream are the links of the response.
 * </ul>
 *
 * <p>The URI templates are not resolved here — the provider does not know the request URIs; the API
 * layer resolves them against the service, collection and feature URIs.
 */
public class FeatureTokenTransformerPropertyLinks extends FeatureTokenTransformer {

  private static final DateTimeFormatter FLEXIBLE_PARSER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd[['T'][' ']HH:mm:ss[.SSS]][X]");

  private final Consumer<List<PropertyLink>> propertyLinksSetter;
  private List<PropertyLink> current = new ArrayList<>();
  private boolean isSingleFeature;
  private List<PropertyLink> resultPropertyLinks;

  public FeatureTokenTransformerPropertyLinks(ImmutableResult.Builder resultBuilder) {
    super();
    this.propertyLinksSetter = resultBuilder::propertyLinks;
  }

  public <X> FeatureTokenTransformerPropertyLinks(ImmutableResultReduced.Builder<X> resultBuilder) {
    super();
    this.propertyLinksSetter = resultBuilder::propertyLinks;
  }

  @Override
  public void onStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    this.isSingleFeature = context.metadata().isSingleFeature();
    super.onStart(context);
  }

  @Override
  public void onFeatureStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    this.current = new ArrayList<>();
    context.setPropertyLinks(List.of());
    super.onFeatureStart(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    Optional<SchemaLink> link = context.schema().flatMap(FeatureSchema::getEffectiveLink);
    if (link.isPresent() && Objects.nonNull(context.value())) {
      FeatureSchema schema = context.schema().get();
      String value =
          schema.getType() == SchemaBase.Type.DATETIME
              ? normalizeToIso(context.value())
              : context.value();
      current.add(
          PropertyLink.of(
              link.get().getRel(), link.get().getUriTemplate(), value, schema.getLabel()));
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
      List<PropertyLink> links = List.copyOf(current);
      context.setPropertyLinks(links);
      this.resultPropertyLinks = links;
    }
    super.onFeatureEnd(context);
  }

  @Override
  public void onEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (isSingleFeature && Objects.nonNull(resultPropertyLinks)) {
      propertyLinksSetter.accept(resultPropertyLinks);
    }
    super.onEnd(context);
  }
}
