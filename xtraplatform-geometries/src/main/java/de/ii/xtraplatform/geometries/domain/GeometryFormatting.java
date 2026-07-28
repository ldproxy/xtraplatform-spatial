/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain;

/**
 * Formatting helpers for validation error messages — keep diagnostics readable when the offending
 * geometry has many positions.
 */
final class GeometryFormatting {

  private GeometryFormatting() {}

  /**
   * Render a flat coordinate array as a positions list like {@code [(x1 y1), (x2 y2), …]}, capped
   * at {@code maxPositions} with an ellipsis-and-count tail when more positions are present.
   */
  static String formatPositions(double[] coordinates, Axes axes, int maxPositions) {
    int dim = axes.size();
    if (coordinates.length == 0 || dim == 0) {
      return "[]";
    }
    int total = coordinates.length / dim;
    int shown = Math.min(total, maxPositions);
    StringBuilder sb = new StringBuilder(shown * 24);
    sb.append('[');
    for (int p = 0; p < shown; p++) {
      if (p > 0) sb.append(", ");
      sb.append('(');
      for (int i = 0; i < dim; i++) {
        if (i > 0) sb.append(' ');
        sb.append(coordinates[p * dim + i]);
      }
      sb.append(')');
    }
    if (total > shown) {
      sb.append(", … (").append(total - shown).append(" more)");
    }
    sb.append(']');
    return sb.toString();
  }
}
