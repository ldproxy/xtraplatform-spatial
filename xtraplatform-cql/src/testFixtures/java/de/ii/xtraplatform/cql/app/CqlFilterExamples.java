/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.cql.domain.AContainedBy;
import de.ii.xtraplatform.cql.domain.AContains;
import de.ii.xtraplatform.cql.domain.AEquals;
import de.ii.xtraplatform.cql.domain.AOverlaps;
import de.ii.xtraplatform.cql.domain.Accenti;
import de.ii.xtraplatform.cql.domain.And;
import de.ii.xtraplatform.cql.domain.ArrayLiteral;
import de.ii.xtraplatform.cql.domain.Bbox;
import de.ii.xtraplatform.cql.domain.Between;
import de.ii.xtraplatform.cql.domain.BinarySpatialOperation;
import de.ii.xtraplatform.cql.domain.BooleanValue2;
import de.ii.xtraplatform.cql.domain.Casei;
import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.cql.domain.CqlFilter;
import de.ii.xtraplatform.cql.domain.Eq;
import de.ii.xtraplatform.cql.domain.Function;
import de.ii.xtraplatform.cql.domain.Gt;
import de.ii.xtraplatform.cql.domain.Gte;
import de.ii.xtraplatform.cql.domain.In;
import de.ii.xtraplatform.cql.domain.Interval;
import de.ii.xtraplatform.cql.domain.IsNull;
import de.ii.xtraplatform.cql.domain.Like;
import de.ii.xtraplatform.cql.domain.Lt;
import de.ii.xtraplatform.cql.domain.Lte;
import de.ii.xtraplatform.cql.domain.Neq;
import de.ii.xtraplatform.cql.domain.Not;
import de.ii.xtraplatform.cql.domain.Or;
import de.ii.xtraplatform.cql.domain.Property;
import de.ii.xtraplatform.cql.domain.SIntersects;
import de.ii.xtraplatform.cql.domain.STouches;
import de.ii.xtraplatform.cql.domain.SWithin;
import de.ii.xtraplatform.cql.domain.ScalarLiteral;
import de.ii.xtraplatform.cql.domain.SpatialFunction;
import de.ii.xtraplatform.cql.domain.SpatialLiteral;
import de.ii.xtraplatform.cql.domain.SpatialOperation;
import de.ii.xtraplatform.cql.domain.TAfter;
import de.ii.xtraplatform.cql.domain.TBefore;
import de.ii.xtraplatform.cql.domain.TDuring;
import de.ii.xtraplatform.cql.domain.TIntersects;
import de.ii.xtraplatform.cql.domain.TemporalFunction;
import de.ii.xtraplatform.cql.domain.TemporalLiteral;
import de.ii.xtraplatform.cql.domain.TemporalOperation;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.geometries.domain.Axes;
import de.ii.xtraplatform.geometries.domain.GeometryCollection;
import de.ii.xtraplatform.geometries.domain.LineString;
import de.ii.xtraplatform.geometries.domain.MultiLineString;
import de.ii.xtraplatform.geometries.domain.MultiPoint;
import de.ii.xtraplatform.geometries.domain.MultiPolygon;
import de.ii.xtraplatform.geometries.domain.Point;
import de.ii.xtraplatform.geometries.domain.Polygon;
import de.ii.xtraplatform.geometries.domain.PositionList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class CqlFilterExamples {

  public static final Cql2Expression EXAMPLE_1 = Gt.of("floors", ScalarLiteral.of(5));
  public static final Cql2Expression EXAMPLE_1_OLD = Gt.of("floors", ScalarLiteral.of(5));

  public static final Cql2Expression EXAMPLE_2 = Lte.of("taxes", ScalarLiteral.of(500));
  public static final CqlFilter EXAMPLE_2_OLD =
      CqlFilter.of(Lte.of("taxes", ScalarLiteral.of(500)));

  public static final Cql2Expression EXAMPLE_3 = Like.of("owner", ScalarLiteral.of("% Jones %"));
  public static final CqlFilter EXAMPLE_3_OLD =
      CqlFilter.of(Like.of("owner", ScalarLiteral.of("% Jones %")));

  public static final Cql2Expression EXAMPLE_4 = Like.of("owner", ScalarLiteral.of("Mike%"));
  public static final CqlFilter EXAMPLE_4_OLD =
      CqlFilter.of(Like.of("owner", ScalarLiteral.of("Mike%")));

  public static final Cql2Expression EXAMPLE_5 =
      Not.of(Like.of("owner", ScalarLiteral.of("% Mike %")));
  public static final Cql2Expression EXAMPLE_5_OLD =
      /*Not.of(*/ Like.of("owner", ScalarLiteral.of("% Mike %")) /*)*/;

  public static final Cql2Expression EXAMPLE_6 = Eq.of("swimming_pool", ScalarLiteral.of(true));
  public static final Cql2Expression EXAMPLE_6_OLD = Eq.of("swimming_pool", ScalarLiteral.of(true));

  public static final Cql2Expression EXAMPLE_7 = And.of(EXAMPLE_1, EXAMPLE_6);
  public static final Cql2Expression EXAMPLE_7_OLD =
      And.of(EXAMPLE_1_OLD, Eq.of("swimming_pool", ScalarLiteral.of(true)));

  public static final Cql2Expression EXAMPLE_8 =
      And.of(
          EXAMPLE_6,
          Or.of(
              EXAMPLE_1,
              Like.of("material", ScalarLiteral.of("brick%")),
              Like.of("material", ScalarLiteral.of("%brick"))));
  public static final Cql2Expression EXAMPLE_8_OLD =
      And.of(
          EXAMPLE_6_OLD,
          Or.of(
              EXAMPLE_1_OLD,
              Like.of("material", ScalarLiteral.of("brick%")),
              Like.of("material", ScalarLiteral.of("%brick"))));

  public static final Cql2Expression EXAMPLE_9 =
      Or.of(And.of(EXAMPLE_1, Eq.of("material", ScalarLiteral.of("brick"))), EXAMPLE_6);
  public static final Cql2Expression EXAMPLE_9_OLD =
      Or.of(And.of(EXAMPLE_1_OLD, Eq.of("material", ScalarLiteral.of("brick"))), EXAMPLE_6_OLD);

  public static final Cql2Expression EXAMPLE_10 =
      Or.of(Not.of(Lt.of("floors", ScalarLiteral.of(5))), EXAMPLE_6);
  public static final Cql2Expression EXAMPLE_10_OLD =
      Or.of(/*Not.of(*/ Lt.of("floors", ScalarLiteral.of(5)) /*)*/, EXAMPLE_6_OLD);

  public static final Cql2Expression EXAMPLE_11 =
      And.of(
          Or.of(
              Like.of("owner", ScalarLiteral.of("mike%")),
              Like.of("owner", ScalarLiteral.of("Mike%"))),
          Lt.of("floors", ScalarLiteral.of(4)));
  public static final Cql2Expression EXAMPLE_11_OLD =
      And.of(
          Or.of(
              Like.of("owner", ScalarLiteral.of("mike%")),
              Like.of("owner", ScalarLiteral.of("Mike%"))),
          Lt.of("floors", ScalarLiteral.of(4)));

  public static final Cql2Expression EXAMPLE_12 =
      TBefore.of(Property.of("built"), TemporalLiteral.of("2012-06-05T00:00:00Z"));
  public static final CqlFilter EXAMPLE_12_OLD =
      CqlFilter.of(
          TemporalOperation.of(
              TemporalFunction.T_BEFORE, "built", TemporalLiteral.of("2012-06-05T00:00:00Z")));
  public static final Cql2Expression EXAMPLE_12_date =
      TBefore.of(Property.of("built"), TemporalLiteral.of("2012-06-05"));

  public static final Cql2Expression EXAMPLE_12_alt =
      Lt.of(ImmutableList.of(Property.of("built"), TemporalLiteral.of("2012-06-05T00:00:00Z")));
  public static final Cql2Expression EXAMPLE_12eq_alt =
      Lte.of(ImmutableList.of(Property.of("built"), TemporalLiteral.of("2012-06-05T00:00:00Z")));

  public static final Cql2Expression EXAMPLE_13 =
      TAfter.of(Property.of("built"), TemporalLiteral.of("2012-06-05T00:00:00Z"));
  public static final CqlFilter EXAMPLE_13_OLD =
      CqlFilter.of(
          TemporalOperation.of(
              TemporalFunction.T_AFTER, "built", TemporalLiteral.of("2012-06-05T00:00:00Z")));

  public static final Cql2Expression EXAMPLE_13_alt =
      Gt.of(ImmutableList.of(Property.of("built"), TemporalLiteral.of("2012-06-05T00:00:00Z")));
  public static final Cql2Expression EXAMPLE_13eq_alt =
      Gte.of(ImmutableList.of(Property.of("built"), TemporalLiteral.of("2012-06-05T00:00:00Z")));

  public static final Cql2Expression EXAMPLE_13A_alt =
      Eq.of(ImmutableList.of(Property.of("built"), TemporalLiteral.of("2012-06-05T00:00:00Z")));
  public static final Cql2Expression EXAMPLE_13Aneq_alt =
      Neq.of(ImmutableList.of(Property.of("built"), TemporalLiteral.of("2012-06-05T00:00:00Z")));

  public static final Cql2Expression EXAMPLE_14 =
      TDuring.of(
          Property.of("updated"),
          TemporalLiteral.of(ImmutableList.of("2017-06-10T07:30:00Z", "2017-06-11T10:30:00Z")));

  public static final Cql2Expression EXAMPLE_14_B =
      TDuring.of(
          TemporalLiteral.of(ImmutableList.of("2017-06-10T07:30:00Z", "2017-06-11T10:30:00Z")),
          Property.of("updated"));

  public static final Cql2Expression EXAMPLE_14_Negation =
      Not.of(
          TDuring.of(
              Property.of("updated"),
              TemporalLiteral.of(
                  ImmutableList.of("2017-06-10T07:30:00Z", "2017-06-11T10:30:00Z"))));
  public static final CqlFilter EXAMPLE_14_OLD =
      CqlFilter.of(
          TemporalOperation.of(
              TemporalFunction.T_DURING,
              "updated",
              TemporalLiteral.of(
                  ImmutableList.of("2017-06-10T07:30:00Z", "2017-06-11T10:30:00Z"))));

  public static final Cql2Expression EXAMPLE_15 =
      SWithin.of(
          Property.of("location"),
          SpatialLiteral.of(Bbox.of(-118.0, 33.8, -117.9, 34.0, OgcCrs.CRS84)));

  public static final Cql2Expression EXAMPLE_15_RandomCrs =
      SWithin.of(
          Property.of("location"),
          SpatialLiteral.of(Bbox.of(-118.0, 33.8, -117.9, 34.0, EpsgCrs.of(8888))));

  public static final CqlFilter EXAMPLE_15_OLD =
      CqlFilter.of(
          SpatialOperation.of(
              SpatialFunction.S_WITHIN,
              "location",
              SpatialLiteral.of(Bbox.of(-118.0, 33.8, -117.9, 34.0, OgcCrs.CRS84))));

  public static final Cql2Expression EXAMPLE_16 =
      SIntersects.of(
          Property.of("location"),
          SpatialLiteral.of(
              Polygon.of(
                  List.of(
                      PositionList.of(
                          Axes.XY,
                          new double[] {-10.0, -10.0, 10.0, -10.0, 10.0, 10.0, -10.0, -10.0})),
                  Optional.of(OgcCrs.CRS84))));

  public static final Cql2Expression EXAMPLE_16_MultiPolygon =
      SIntersects.of(
          Property.of("location"),
          SpatialLiteral.of(
              MultiPolygon.of(
                  List.of(
                      Polygon.of(
                          List.of(
                              PositionList.of(
                                  Axes.XY,
                                  new double[] {
                                    -10.0, -10.0, 10.0, -10.0, 10.0, 10.0, -10.0, -10.0
                                  })),
                          Optional.of(OgcCrs.CRS84)),
                      Polygon.of(
                          List.of(
                              PositionList.of(
                                  Axes.XY,
                                  new double[] {
                                    -15.0, -15.0, 15.0, -15.0, 15.0, 15.0, -15.0, -15.0
                                  })),
                          Optional.of(OgcCrs.CRS84))),
                  Optional.of(OgcCrs.CRS84))));

  public static final Cql2Expression EXAMPLE_16_MultiLineString =
      SIntersects.of(
          Property.of("location"),
          SpatialLiteral.of(
              MultiLineString.of(
                  List.of(
                      LineString.of(
                          PositionList.of(
                              Axes.XY,
                              new double[] {-10.0, -10.0, 10.0, -10.0, 10.0, 10.0, -10.0, -10.0}),
                          Optional.of(OgcCrs.CRS84)),
                      LineString.of(
                          PositionList.of(
                              Axes.XY,
                              new double[] {-15.0, -15.0, 15.0, -15.0, 15.0, 15.0, -15.0, -15.0}),
                          Optional.of(OgcCrs.CRS84))),
                  Optional.of(OgcCrs.CRS84))));
  public static final Cql2Expression EXAMPLE_16_LineString =
      SIntersects.of(
          Property.of("location"),
          SpatialLiteral.of(
              LineString.of(
                  PositionList.of(
                      Axes.XY, new double[] {-10.0, -10.0, 10.0, -10.0, 10.0, 10.0, -10.0, -10.0}),
                  Optional.of(OgcCrs.CRS84))));

  public static final Cql2Expression EXAMPLE_16_Point =
      SIntersects.of(Property.of("location"), SpatialLiteral.of(Point.of(10, -10, OgcCrs.CRS84)));

  public static final Cql2Expression EXAMPLE_16_MultiPoint =
      SIntersects.of(
          Property.of("location"),
          SpatialLiteral.of(
              MultiPoint.of(
                  List.of(Point.of(10, -10, OgcCrs.CRS84), Point.of(10, 10, OgcCrs.CRS84)),
                  Optional.of(OgcCrs.CRS84))));

  public static final Cql2Expression EXAMPLE_16_BBox =
      SIntersects.of(
          Property.of("location"),
          SpatialLiteral.of(Bbox.of(-10.0, -10.0, 10.0, 10.0, OgcCrs.CRS84)));

  public static final Cql2Expression EXAMPLE_16_GeometryCollection =
      SIntersects.of(
          Property.of("location"),
          SpatialLiteral.of(
              GeometryCollection.of(
                  List.of(Point.of(10, -10, OgcCrs.CRS84), Point.of(10, 10, OgcCrs.CRS84)),
                  Optional.of(OgcCrs.CRS84))));

  public static final Cql2Expression EXAMPLE_16_OLD =
      SIntersects.of(
          Property.of("location"),
          SpatialLiteral.of(
              Polygon.of(
                  List.of(
                      PositionList.of(
                          Axes.XY,
                          new double[] {-10.0, -10.0, 10.0, -10.0, 10.0, 10.0, -10.0, -10.0})),
                  Optional.of(OgcCrs.CRS84))));

  public static final Cql2Expression EXAMPLE_17 =
      And.of(
          EXAMPLE_1,
          SWithin.of(
              Property.of("geometry"),
              SpatialLiteral.of(Bbox.of(-118.0, 33.8, -117.9, 34.0, OgcCrs.CRS84))));
  public static final Cql2Expression EXAMPLE_17_OLD =
      And.of(
          EXAMPLE_1_OLD,
          SWithin.of(
              Property.of("geometry"),
              SpatialLiteral.of(Bbox.of(-118.0, 33.8, -117.9, 34.0, OgcCrs.CRS84))));

  public static final Cql2Expression EXAMPLE_18 =
      Between.of(Property.of("floors"), ScalarLiteral.of(4), ScalarLiteral.of(8));
  public static final CqlFilter EXAMPLE_18_OLD =
      CqlFilter.of(Between.of("floors", ScalarLiteral.of(4), ScalarLiteral.of(8)));

  public static final Cql2Expression EXAMPLE_19 =
      In.of("owner", ScalarLiteral.of("Mike"), ScalarLiteral.of("John"), ScalarLiteral.of("Tom"));
  public static final CqlFilter EXAMPLE_19_OLD =
      CqlFilter.of(
          In.of(
              "owner",
              ScalarLiteral.of("Mike"),
              ScalarLiteral.of("John"),
              ScalarLiteral.of("Tom")));
  public static final Cql2Expression EXAMPLE_20 = IsNull.of("owner");
  public static final CqlFilter EXAMPLE_20_OLD = CqlFilter.of(IsNull.of("owner"));

  public static final Cql2Expression EXAMPLE_21 = Not.of(IsNull.of("owner"));
  public static final CqlFilter EXAMPLE_21_OLD = CqlFilter.of(Not.of(IsNull.of("owner")));

  public static final Cql2Expression EXAMPLE_24 =
      TBefore.of(Property.of("built"), TemporalLiteral.of("2015-01-01"));
  public static final CqlFilter EXAMPLE_24_OLD =
      CqlFilter.of(
          TemporalOperation.of(
              TemporalFunction.T_BEFORE, "built", TemporalLiteral.of("2015-01-01")));

  public static final Cql2Expression EXAMPLE_25 =
      TDuring.of(
          Property.of("updated"),
          Objects.requireNonNull(
              TemporalLiteral.of("2017-06-10T07:30:00Z", "2017-06-11T10:30:00Z")));
  public static final Cql2Expression EXAMPLE_25b =
      TDuring.of(
          Property.of("updated"),
          Objects.requireNonNull(TemporalLiteral.of("2017-06-10", "2017-06-11")));
  public static final Cql2Expression EXAMPLE_25x =
      TIntersects.of(
          Interval.of(ImmutableList.of(Property.of("start"), Property.of("end"))),
          Objects.requireNonNull(
              TemporalLiteral.of("2017-06-10T07:30:00Z", "2017-06-11T10:30:00Z")));
  public static final Cql2Expression EXAMPLE_25y =
      TIntersects.of(
          Interval.of(ImmutableList.of(Property.of("start"), Property.of("end"))),
          Objects.requireNonNull(TemporalLiteral.of("2017-06-10", "2017-06-11")));
  public static final Cql2Expression EXAMPLE_25z =
      TIntersects.of(
          Interval.of(ImmutableList.of(Property.of("start"), TemporalLiteral.of(".."))),
          Objects.requireNonNull(TemporalLiteral.of("2017-06-10", "..")));

  public static final Cql2Expression EXAMPLE_Interval =
      TIntersects.of(
          Objects.requireNonNull(
              TemporalLiteral.of("2017-06-10T07:30:00Z", "2017-06-11T10:30:00Z")),
          Interval.of(
              ImmutableList.of(TemporalLiteral.of("2012-06-05T00:00:00Z"), Property.of("end"))));

  public static final Cql2Expression EXAMPLE_Illegal_Interval =
      TIntersects.of(
          Objects.requireNonNull(
              TemporalLiteral.of("2017-06-10T07:30:00Z", "2017-06-11T10:30:00Z")),
          Interval.of(
              ImmutableList.of(
                  TemporalLiteral.of("2012-06-05T00:00:00Z"),
                  TemporalLiteral.of("2017-06-10T07:30:00Z"))));
  public static final CqlFilter EXAMPLE_25_OLD =
      CqlFilter.of(
          TemporalOperation.of(
              TemporalFunction.T_DURING,
              "updated",
              Objects.requireNonNull(TemporalLiteral.of("2017-06-10", "2017-06-11"))));

  public static final Cql2Expression EXAMPLE_26 =
      TDuring.of(
          Property.of("updated"),
          Objects.requireNonNull(TemporalLiteral.of("2017-06-10T07:30:00Z", "..")));
  public static final CqlFilter EXAMPLE_26_OLD =
      CqlFilter.of(
          TemporalOperation.of(
              TemporalFunction.T_DURING,
              "updated",
              Objects.requireNonNull(TemporalLiteral.of("2017-06-10T07:30:00Z", ".."))));

  public static final Cql2Expression EXAMPLE_27 =
      TDuring.of(
          Property.of("updated"),
          Objects.requireNonNull(TemporalLiteral.of("..", "2017-06-11T10:30:00Z")));
  public static final CqlFilter EXAMPLE_27_OLD =
      CqlFilter.of(
          TemporalOperation.of(
              TemporalFunction.T_DURING,
              "updated",
              Objects.requireNonNull(TemporalLiteral.of("..", "2017-06-11T10:30:00Z"))));

  public static final Cql2Expression EXAMPLE_28 =
      TDuring.of(Property.of("updated"), Objects.requireNonNull(TemporalLiteral.of("..", "..")));
  public static final CqlFilter EXAMPLE_28_OLD =
      CqlFilter.of(
          TemporalOperation.of(
              TemporalFunction.T_DURING,
              "updated",
              Objects.requireNonNull(TemporalLiteral.of("..", ".."))));

  public static final Cql2Expression EXAMPLE_29 =
      Eq.ofFunction(Function.of("pos", ImmutableList.of()), ScalarLiteral.of(1));
  public static final CqlFilter EXAMPLE_29_OLD =
      CqlFilter.of(Eq.ofFunction(Function.of("pos", ImmutableList.of()), ScalarLiteral.of(1)));

  public static final Cql2Expression EXAMPLE_30 =
      Gte.ofFunction(
          Function.of("indexOf", ImmutableList.of(Property.of("names"), ScalarLiteral.of("Mike"))),
          ScalarLiteral.of(5));
  public static final CqlFilter EXAMPLE_30_OLD =
      CqlFilter.of(
          Gte.ofFunction(
              Function.of(
                  "indexOf", ImmutableList.of(Property.of("names"), ScalarLiteral.of("Mike"))),
              ScalarLiteral.of(5)));

  public static final Cql2Expression EXAMPLE_31 =
      Eq.ofFunction(
          Function.of(
              "year",
              ImmutableList.of(Objects.requireNonNull(TemporalLiteral.of("2012-06-05T00:00:00Z")))),
          ScalarLiteral.of(2012));
  public static final CqlFilter EXAMPLE_31_OLD =
      CqlFilter.of(
          Eq.ofFunction(
              Function.of(
                  "year",
                  ImmutableList.of(
                      Objects.requireNonNull(TemporalLiteral.of("2012-06-05T00:00:00Z")))),
              ScalarLiteral.of(2012)));

  public static final Cql2Expression EXAMPLE_32 =
      Gt.of(
          Property.of(
              "filterValues.measure",
              ImmutableMap.of(
                  "filterValues", Eq.of("filterValues.property", ScalarLiteral.of("d30")))),
          ScalarLiteral.of(0.1));
  public static final CqlFilter EXAMPLE_32_OLD =
      CqlFilter.of(
          Gt.of(
              Property.of(
                  "filterValues.measure",
                  ImmutableMap.of(
                      "filterValues", Eq.of("filterValues.property", ScalarLiteral.of("d30")))),
              ScalarLiteral.of(0.1)));

  public static final Cql2Expression EXAMPLE_33 =
      Gt.of(
          Property.of(
              "filterValues1.filterValues2.measure",
              ImmutableMap.of(
                  "filterValues1",
                  Eq.of("filterValues1.property1", ScalarLiteral.of("d30")),
                  "filterValues2",
                  Lte.of("filterValues2.property2", ScalarLiteral.of(100)))),
          ScalarLiteral.of(0.1));

  public static final Cql2Expression EXAMPLE_41 =
      Eq.of(
          Property.of(
              "filterValues.classification",
              ImmutableMap.of(
                  "filterValues",
                  Eq.of("filterValues.property", ScalarLiteral.of("Bodenklassifizierung")))),
          ScalarLiteral.of("GU/GT"));
  public static final CqlFilter EXAMPLE_41_OLD =
      CqlFilter.of(
          Eq.of(
              Property.of(
                  "filterValues.classification",
                  ImmutableMap.of(
                      "filterValues",
                      Eq.of("filterValues.property", ScalarLiteral.of("Bodenklassifizierung")))),
              ScalarLiteral.of("GU/GT")));

  public static final Cql2Expression EXAMPLE_42 = Or.of(EXAMPLE_32, EXAMPLE_41);

  public static final Cql2Expression EXAMPLE_34 =
      Eq.of("landsat:scene_id", ScalarLiteral.of("LC82030282019133LGN00"));
  public static final CqlFilter EXAMPLE_34_OLD =
      CqlFilter.of(Eq.of("landsat:scene_id", ScalarLiteral.of("LC82030282019133LGN00")));

  public static final CqlFilter EXAMPLE_35 =
      CqlFilter.of(Like.of("name", ScalarLiteral.of("Smith.")));

  public static final CqlFilter EXAMPLE_36 =
      CqlFilter.of(
          TemporalOperation.of(
              TemporalFunction.T_INTERSECTS,
              "event_date",
              TemporalLiteral.of("1969-07-16T05:32:00Z", "1969-07-24T16:50:35Z")));

  public static final Cql2Expression EXAMPLE_37 = Lt.of("height", "floors");
  public static final CqlFilter EXAMPLE_37_OLD = CqlFilter.of(Lt.of("height", "floors"));

  public static final Cql2Expression EXAMPLE_38 =
      AContains.of(
          Property.of("layer:ids"),
          ArrayLiteral.of(
              ImmutableList.of(ScalarLiteral.of("layers-ca"), ScalarLiteral.of("layers-us"))));

  public static final Cql2Expression EXAMPLE_39 =
      Not.of(Between.of("floors", ScalarLiteral.of(4), ScalarLiteral.of(8)));
  public static final CqlFilter EXAMPLE_39_OLD =
      CqlFilter.of(Not.of(Between.of("floors", ScalarLiteral.of(4), ScalarLiteral.of(8))));

  public static final Cql2Expression EXAMPLE_40 =
      Not.of(
          In.of(
              "owner",
              ScalarLiteral.of("Mike"),
              ScalarLiteral.of("John"),
              ScalarLiteral.of("Tom")));
  public static final CqlFilter EXAMPLE_40_OLD =
      CqlFilter.of(
          Not.of(
              In.of(
                  "owner",
                  ScalarLiteral.of("Mike"),
                  ScalarLiteral.of("John"),
                  ScalarLiteral.of("Tom"))));

  public static final Cql2Expression EXAMPLE_43 =
      Between.ofFunction(
          Function.of("position", ImmutableList.of()), ScalarLiteral.of(4), ScalarLiteral.of(8));

  public static final CqlFilter EXAMPLE_43_OLD =
      CqlFilter.of(
          Between.ofFunction(
              Function.of("position", ImmutableList.of()),
              ScalarLiteral.of(4),
              ScalarLiteral.of(8)));

  public static final Cql2Expression EXAMPLE_44 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of("theme", Like.of("theme.scheme", ScalarLiteral.of("profile")))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final Cql2Expression EXAMPLE_45 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of(
                  "theme",
                  TIntersects.of(
                      Property.of("theme.event"),
                      Interval.of(
                          ImmutableList.of(
                              Property.of("theme.start_date"), Property.of("theme.end_date")))))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final Cql2Expression EXAMPLE_46 =
      AContains.of(
          Property.of(
              "theme.concept", ImmutableMap.of("theme", IsNull.of(Property.of("theme.scheme")))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final Cql2Expression EXAMPLE_47 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of(
                  "theme",
                  In.of(
                      Casei.of(Property.of("theme.schema")),
                      ImmutableList.of(
                          Casei.of(ScalarLiteral.of("region")),
                          Casei.of(ScalarLiteral.of("straße")))))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final Cql2Expression EXAMPLE_48 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of(
                  "theme",
                  In.of(
                      Accenti.of(Property.of("theme.schema")),
                      ImmutableList.of(
                          Accenti.of(ScalarLiteral.of("region")),
                          Accenti.of(ScalarLiteral.of("straße")))))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final Cql2Expression EXAMPLE_49 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of(
                  "theme",
                  Or.of(
                      Eq.of(
                          ImmutableList.of(
                              Property.of("theme.schema"), ScalarLiteral.of("schema_1"))),
                      Eq.of(
                          ImmutableList.of(
                              Property.of("theme.schema"), ScalarLiteral.of("schema_2")))))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final Cql2Expression EXAMPLE_50 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of(
                  "theme",
                  And.of(
                      Gt.of(ImmutableList.of(Property.of("theme.length"), ScalarLiteral.of(5))),
                      Gt.of(ImmutableList.of(Property.of("theme.count"), ScalarLiteral.of(10)))))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final Cql2Expression EXAMPLE_51 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of("theme", Lt.of(Property.of("theme.length"), ScalarLiteral.of(5)))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final Cql2Expression EXAMPLE_52 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of("theme", Gte.of(Property.of("theme.length"), ScalarLiteral.of(5)))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final Cql2Expression EXAMPLE_53 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of("theme", Lte.of(Property.of("theme.length"), ScalarLiteral.of(5)))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final Cql2Expression EXAMPLE_54 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of("theme", Neq.of(Property.of("theme.length"), ScalarLiteral.of(5)))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final Cql2Expression EXAMPLE_55 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of(
                  "theme", Not.of(Eq.of(Property.of("theme.length"), ScalarLiteral.of(5))))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final Cql2Expression EXAMPLE_56 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of(
                  "theme",
                  BinarySpatialOperation.of(
                      SpatialFunction.S_TOUCHES,
                      Property.of("theme.event"),
                      Property.of("theme.location_geometry")))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));
  public static final Cql2Expression EXAMPLE_57 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of(
                  "theme",
                  AOverlaps.of(
                      Property.of("theme.event"), Property.of("theme.location_geometry")))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));
  public static final Cql2Expression EXAMPLE_58 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of(
                  "theme",
                  TBefore.of(
                      Property.of("theme.built"), TemporalLiteral.of("2012-06-05T00:00:00Z")))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final Cql2Expression EXAMPLE_59 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of(
                  "theme",
                  SWithin.of(
                      Property.of("theme.location"),
                      SpatialLiteral.of(Bbox.of(-118.0, 33.8, -117.9, 34.0, OgcCrs.CRS84))))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final Cql2Expression EXAMPLE_60 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of(
                  "theme",
                  Eq.of(
                      ImmutableList.of(
                          Property.of("theme.road_class"), Property.of("theme.name"))))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final Cql2Expression EXAMPLE_61 =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of(
                  "theme",
                  SWithin.of(
                      SpatialLiteral.of(Point.of(1.00, 1.00, OgcCrs.CRS84)),
                      SpatialLiteral.of(
                          Polygon.of(
                              new double[] {-10.0, -10.0, 10.0, -10.0, 10.0, 10.0, -10.0, -10.0},
                              OgcCrs.CRS84))))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final CqlFilter EXAMPLE_TEQUALS =
      CqlFilter.of(
          Eq.of(
              ImmutableList.of(Property.of("built"), TemporalLiteral.of("2012-06-05T00:00:00Z"))));

  public static final CqlFilter EXAMPLE_TDISJOINT =
      CqlFilter.of(
          TemporalOperation.of(
              TemporalFunction.T_DISJOINT,
              "event_date",
              TemporalLiteral.of("1969-07-16T05:32:00Z", "1969-07-24T16:50:35Z")));

  public static final Cql2Expression EXAMPLE_TINTERSECTS =
      TIntersects.of(
          Property.of("event_date"),
          Interval.of(ImmutableList.of(Property.of("startDate"), Property.of("endDate"))));

  public static final CqlFilter EXAMPLE_SDISJOINT =
      CqlFilter.of(
          SpatialOperation.of(
              SpatialFunction.S_DISJOINT,
              "geometry",
              SpatialLiteral.of(Bbox.of(-118.0, 33.8, -117.9, 34.0))));

  public static final CqlFilter EXAMPLE_SEQUALS =
      CqlFilter.of(
          SpatialOperation.of(
              SpatialFunction.S_EQUALS,
              "geometry",
              SpatialLiteral.of(Bbox.of(-118.0, 33.8, -117.9, 34.0))));

  public static final CqlFilter EXAMPLE_STOUCHES =
      CqlFilter.of(
          SpatialOperation.of(
              SpatialFunction.S_TOUCHES,
              "geometry",
              SpatialLiteral.of(Bbox.of(-118.0, 33.8, -117.9, 34.0))));

  public static final CqlFilter EXAMPLE_SOVERLAPS =
      CqlFilter.of(
          SpatialOperation.of(
              SpatialFunction.S_OVERLAPS,
              "geometry",
              SpatialLiteral.of(Bbox.of(-118.0, 33.8, -117.9, 34.0))));

  public static final CqlFilter EXAMPLE_SCROSSES =
      CqlFilter.of(
          SpatialOperation.of(
              SpatialFunction.S_CROSSES,
              "geometry",
              SpatialLiteral.of(Bbox.of(-118.0, 33.8, -117.9, 34.0))));

  public static final CqlFilter EXAMPLE_SCONTAINS =
      CqlFilter.of(
          SpatialOperation.of(
              SpatialFunction.S_CONTAINS,
              "geometry",
              SpatialLiteral.of(Bbox.of(-118.0, 33.8, -117.9, 34.0))));

  public static final CqlFilter EXAMPLE_NESTED_TEMPORAL =
      CqlFilter.of(
          Gt.of(
              Property.of(
                  "filterValues.measure",
                  ImmutableMap.of(
                      "filterValues",
                      Gt.of(
                          ImmutableList.of(
                              Property.of("filterValues.updated"),
                              TemporalLiteral.of("2012-06-05T00:00:00Z"))))),
              ScalarLiteral.of(0.1)));

  public static final CqlFilter EXAMPLE_NESTED_SPATIAL_OLD =
      CqlFilter.of(
          Gt.of(
              Property.of(
                  "filterValues.measure",
                  ImmutableMap.of(
                      "filterValues",
                      STouches.of(
                          Property.of("filterValues.location"),
                          SpatialLiteral.of(Bbox.of(-118.0, 33.8, -117.9, 34.0))))),
              ScalarLiteral.of(0.1)));

  public static final Cql2Expression EXAMPLE_IN_WITH_FUNCTION =
      In.ofFunction(
          Function.of("position", ImmutableList.of()),
          ImmutableList.of(ScalarLiteral.of(1), ScalarLiteral.of(3)));
  public static final CqlFilter EXAMPLE_IN_WITH_FUNCTION_OLD =
      CqlFilter.of(
          In.ofFunction(
              Function.of("position", ImmutableList.of()),
              ImmutableList.of(ScalarLiteral.of(1), ScalarLiteral.of(3))));

  public static final Cql2Expression EXAMPLE_NESTED_FUNCTION =
      Between.of(
          Property.of(
              "filterValues.measure", ImmutableMap.of("filterValues", EXAMPLE_IN_WITH_FUNCTION)),
          ScalarLiteral.of(1),
          ScalarLiteral.of(5));

  public static final Cql2Expression EXAMPLE_NESTED_FUNCTION_BETWEEN =
      Between.of(
          Property.of("filterValues.measure", ImmutableMap.of("filterValues", EXAMPLE_43)),
          ScalarLiteral.of(1),
          ScalarLiteral.of(5));

  public static final CqlFilter EXAMPLE_NESTED_FUNCTION_OLD =
      CqlFilter.of(
          Between.of(
              Property.of(
                  "filterValues.measure",
                  ImmutableMap.of("filterValues", EXAMPLE_IN_WITH_FUNCTION)),
              ScalarLiteral.of(1),
              ScalarLiteral.of(5)));

  public static final Cql2Expression EXAMPLE_NESTED_WITH_ARRAYS =
      AContains.of(
          Property.of(
              "theme.concept",
              ImmutableMap.of("theme", Eq.of("theme.scheme", ScalarLiteral.of("profile")))),
          ArrayLiteral.of(
              ImmutableList.of(
                  ScalarLiteral.of("DLKM"),
                  ScalarLiteral.of("Basis-DLM"),
                  ScalarLiteral.of("DLM50"))));

  public static final Cql2Expression EXAMPLE_IN_WITH_TEMPORAL =
      In.of(
          "updated",
          TemporalLiteral.of("2017-06-10T07:30:00Z"),
          TemporalLiteral.of("2018-06-10T07:30:00Z"),
          TemporalLiteral.of("2019-06-10T07:30:00Z"),
          TemporalLiteral.of("2020-06-10T07:30:00Z"));

  public static final Cql2Expression EXAMPLE_TRUE = BooleanValue2.of(true);

  public static final Cql2Expression EXAMPLE_BOOLEAN_VALUES =
      And.of(
          BooleanValue2.of(true), Or.of(BooleanValue2.of(false), Not.of(BooleanValue2.of(false))));

  public static final Cql2Expression EXAMPLE_KEYWORD =
      Gt.of(ImmutableList.of(Property.of("root.date"), TemporalLiteral.of("2022-04-17")));

  public static final Cql2Expression EXAMPLE_CASEI =
      In.of(
          Casei.of(Property.of("road_class")),
          ImmutableList.of(
              Casei.of(ScalarLiteral.of("Οδος")), Casei.of(ScalarLiteral.of("Straße"))));
  public static final CqlFilter EXAMPLE_CASEI_OLD =
      CqlFilter.of(
          In.of(
              Casei.of(Property.of("road_class")),
              ImmutableList.of(
                  Casei.of(ScalarLiteral.of("Οδος")), Casei.of(ScalarLiteral.of("Straße")))));

  public static final Cql2Expression EXAMPLE_ACCENTI =
      In.of(
          Accenti.of(Property.of("road_class")),
          ImmutableList.of(
              Accenti.of(ScalarLiteral.of("Οδος")), Accenti.of(ScalarLiteral.of("Straße"))));
  public static final CqlFilter EXAMPLE_ACCENTI_OLD =
      CqlFilter.of(
          In.of(
              Accenti.of(Property.of("road_class")),
              ImmutableList.of(
                  Accenti.of(ScalarLiteral.of("Οδος")), Accenti.of(ScalarLiteral.of("Straße")))));

  public static final Cql2Expression EXAMPLE_UPPER =
      In.ofFunction(
          Function.of("upper", ImmutableList.of(Property.of("road_class"))),
          ImmutableList.of(
              ScalarLiteral.of("A"),
              ScalarLiteral.of("B"),
              ScalarLiteral.of("L"),
              ScalarLiteral.of("K")));
  public static final Cql2Expression EXAMPLE_LOWER =
      In.ofFunction(
          Function.of("lower", ImmutableList.of(Property.of("road_class"))),
          ImmutableList.of(
              ScalarLiteral.of("a"),
              ScalarLiteral.of("b"),
              ScalarLiteral.of("l"),
              ScalarLiteral.of("k")));

  public static final Cql2Expression EXAMPLE_AContains_ValidFor_JOINED_GEOMETRY =
      AContains.of(
          Property.of("location"),
          ArrayLiteral.of(ImmutableList.of(ScalarLiteral.of("id"), ScalarLiteral.of("location"))));

  public static final Cql2Expression EXAMPLE_AEquals_ValidFor_JOINED_GEOMETRY =
      AEquals.of(
          Property.of("location"),
          ArrayLiteral.of(ImmutableList.of(ScalarLiteral.of("id"), ScalarLiteral.of("location"))));

  public static final Cql2Expression EXAMPLE_AOverlaps_ValidFor_JOINED_GEOMETRY =
      AOverlaps.of(
          Property.of("location"),
          ArrayLiteral.of(ImmutableList.of(ScalarLiteral.of("id"), ScalarLiteral.of("location"))));

  public static final Cql2Expression EXAMPLE_AContainedBy_ValidFor_JOINED_GEOMETRY =
      AContainedBy.of(
          Property.of("location"),
          ArrayLiteral.of(ImmutableList.of(ScalarLiteral.of("id"), ScalarLiteral.of("location"))));

  public static final Cql2Expression EXAMPLE_NOT =
      Not.of(
          And.of(
              Not.of(Eq.of(Property.of("test"), ScalarLiteral.of(1))),
              Or.of(
                  Eq.of(Property.of("test1"), ScalarLiteral.of(1)),
                  Neq.of(Property.of("test2"), ScalarLiteral.of("foo")),
                  Gt.of(Property.of("test3"), ScalarLiteral.of("bar"))),
              And.of(
                  Eq.of(Property.of("test1"), ScalarLiteral.of(1)),
                  Neq.of(Property.of("test2"), ScalarLiteral.of("foo")),
                  Gt.of(Property.of("test3"), ScalarLiteral.of("bar"))),
              Eq.of(Property.of("test"), ScalarLiteral.of(1))));
}
