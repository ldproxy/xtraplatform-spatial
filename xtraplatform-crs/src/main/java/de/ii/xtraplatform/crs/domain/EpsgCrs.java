/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.crs.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(builder = "new")
@JsonDeserialize(builder = ImmutableEpsgCrs.Builder.class)
// TODO: test
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public interface EpsgCrs {

  enum Force {
    NONE,
    LON_LAT,
    LAT_LON
  }

  static EpsgCrs of(int code) {
    return ImmutableEpsgCrs.of(code);
  }

  static EpsgCrs of(int code, Force force) {
    return new ImmutableEpsgCrs.Builder().code(code).forceAxisOrder(force).build();
  }

  static EpsgCrs of(int code, int verticalCode) {
    return new ImmutableEpsgCrs.Builder().code(code).verticalCode(verticalCode).build();
  }

  static EpsgCrs of(int code, int verticalCode, Force force) {
    return new ImmutableEpsgCrs.Builder()
        .code(code)
        .verticalCode(verticalCode)
        .forceAxisOrder(force)
        .build();
  }

  static EpsgCrs fromString(String prefixedCode) {
    Optional<EpsgCrs> ogcCrs = OgcCrs.fromString(prefixedCode);
    if (ogcCrs.isPresent()) {
      return ogcCrs.get();
    }

    int code;
    try {
      code = Integer.parseInt(prefixedCode.substring(prefixedCode.lastIndexOf(":") + 1));
    } catch (NumberFormatException e) {
      try {
        code = Integer.parseInt(prefixedCode.substring(prefixedCode.lastIndexOf("/") + 1));
      } catch (NumberFormatException e2) {
        try {
          code = Integer.parseInt(prefixedCode);
        } catch (NumberFormatException e3) {
          throw new IllegalArgumentException("Could not parse CRS: " + prefixedCode);
        }
      }
    }
    return ImmutableEpsgCrs.of(code);
  }

  static EpsgCrs fromString(String prefixedCode, String prefixedCodeVertical) {
    EpsgCrs crs = fromString(prefixedCode);
    EpsgCrs verticalCrs = fromString(prefixedCodeVertical);
    return new ImmutableEpsgCrs.Builder()
        .code(crs.getCode())
        .verticalCode(verticalCrs.getCode())
        .build();
  }

  @Value.Parameter
  int getCode();

  OptionalInt getVerticalCode();

  @Value.Default
  default Force getForceAxisOrder() {
    return Force.NONE;
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isCompoundCrs() {
    return getVerticalCode().isPresent();
  }

  @JsonIgnore
  @Value.Lazy
  default String toSimpleString() {
    return String.format("EPSG:%d", getCode());
  }

  @JsonIgnore
  @Value.Lazy
  default List<String> toSimpleStrings() {
    return getVerticalCode().isPresent()
        ? List.of(toSimpleString(), String.format("EPSG:%d", getVerticalCode().getAsInt()))
        : List.of(toSimpleString());
  }

  @JsonIgnore
  @Value.Lazy
  default String toUrnString() {
    if (Objects.equals(this, OgcCrs.CRS84) || Objects.equals(this, OgcCrs.CRS84h)) {
      return String.format("urn:ogc:def:crs:OGC::%d", getCode());
    }
    return String.format("urn:ogc:def:crs:EPSG::%d", getCode());
  }

  @JsonIgnore
  @Value.Lazy
  default List<String> toUrnStrings() {
    return getVerticalCode().isPresent()
        ? List.of(
            toUrnString(), String.format("urn:ogc:def:crs:EPSG::%d", getVerticalCode().getAsInt()))
        : List.of(toUrnString());
  }

  @JsonIgnore
  @Value.Lazy
  default String toUriString() {
    if (Objects.equals(this, OgcCrs.CRS84)) {
      return OgcCrs.CRS84_URI;
    }
    if (Objects.equals(this, OgcCrs.CRS84h)) {
      return OgcCrs.CRS84h_URI;
    }
    return String.format("http://www.opengis.net/def/crs/EPSG/0/%d", getCode());
  }

  @JsonIgnore
  @Value.Lazy
  default Optional<String> toAlternativeUriString() {
    return Objects.equals(this, OgcCrs.CRS84)
        ? Optional.of(OgcCrs.CRS84_URI_NEW)
        : Optional.empty();
  }

  @JsonIgnore
  @Value.Lazy
  default List<String> toUriStrings() {
    return getVerticalCode().isPresent()
        ? List.of(
            toUriString(),
            String.format("http://www.opengis.net/def/crs/EPSG/0/%d", getVerticalCode().getAsInt()))
        : List.of(toUriString());
  }

  @JsonIgnore
  @Value.Lazy
  default String toSafeCurie() {
    if (Objects.equals(this, OgcCrs.CRS84)) {
      return OgcCrs.CRS84_CURIE;
    }
    if (Objects.equals(this, OgcCrs.CRS84h)) {
      return OgcCrs.CRS84h_CURIE;
    }
    return String.format("[EPSG:%d]", getCode());
  }

  @JsonIgnore
  @Value.Lazy
  default List<String> toSafeCuries() {
    return getVerticalCode().isPresent()
        ? List.of(toSafeCurie(), String.format("[EPSG:%d]", getVerticalCode().getAsInt()))
        : List.of(toSafeCurie());
  }

  @JsonIgnore
  @Value.Lazy
  default String toHumanReadableString() {
    String lonlat =
        getForceAxisOrder() == Force.LON_LAT ? String.format(" (%s)", Force.LON_LAT) : "";
    String vertical =
        getVerticalCode().isPresent()
            ? String.format(" + EPSG:%d", getVerticalCode().getAsInt())
            : "";
    return toSimpleString() + lonlat + vertical;
  }
}
