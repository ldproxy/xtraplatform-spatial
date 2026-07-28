/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import jakarta.ws.rs.core.MediaType;

public enum TilesFormat {
  MVT("pbf", "mvt", MediaType.valueOf("application/vnd.mapbox-vector-tile")),
  JPEG("jpg", "jpeg", MediaType.valueOf("image/jpeg")),
  PNG("png", "png", MediaType.valueOf("image/png")),
  WebP("webp", "webp", MediaType.valueOf("image/webp")),
  TIFF("tiff", "tiff", MediaType.valueOf("image/tiff"));

  private final String mbtilesString;
  private final String fString;
  private final MediaType mediaType;

  TilesFormat(String mbtilesString, String fString, MediaType mediaType) {
    this.mbtilesString = mbtilesString;
    this.fString = fString;
    this.mediaType = mediaType;
  }

  public String asMbtilesString() {
    return mbtilesString;
  }

  public String asFString() {
    return fString;
  }

  public MediaType asMediaType() {
    return mediaType;
  }

  public boolean isRaster() {
    return this != MVT;
  }

  public boolean isVector() {
    return this == MVT;
  }

  public static TilesFormat of(String value) {
    switch (value) {
      case "mvt":
      case "MVT":
      case "pbf":
      case "PBF":
        return MVT;
      case "jpg":
      case "JPG":
      case "jpeg":
      case "JPEG":
        return JPEG;
      case "png":
      case "PNG":
        return PNG;
      case "webp":
      case "WebP":
      case "WEBP":
        return WebP;
      case "tiff":
      case "TIFF":
        return TIFF;
    }
    return null;
  }
}
