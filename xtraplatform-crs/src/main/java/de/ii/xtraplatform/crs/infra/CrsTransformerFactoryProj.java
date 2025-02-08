/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.crs.infra;

import com.github.azahnen.dagger.annotations.AutoBind;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.base.domain.AppContext;
import de.ii.xtraplatform.base.domain.AppLifeCycle;
import de.ii.xtraplatform.base.domain.LogContext;
import de.ii.xtraplatform.base.domain.resiliency.AbstractVolatile;
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry;
import de.ii.xtraplatform.blobs.domain.ResourceStore;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.CrsInfo;
import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.proj.domain.ProjLoader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.measure.Unit;
import org.kortforsyningen.proj.CoordinateOperationContext;
import org.kortforsyningen.proj.GridAvailabilityUse;
import org.kortforsyningen.proj.Proj;
import org.kortforsyningen.proj.SpatialCriterion;
import org.kortforsyningen.proj.Units;
import org.kortforsyningen.proj.spi.EPSG;
import org.opengis.metadata.extent.Extent;
import org.opengis.metadata.extent.GeographicBoundingBox;
import org.opengis.metadata.extent.GeographicExtent;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CRSFactory;
import org.opengis.referencing.crs.CompoundCRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.crs.GeographicCRS;
import org.opengis.referencing.crs.SingleCRS;
import org.opengis.referencing.cs.AxisDirection;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.cs.EllipsoidalCS;
import org.opengis.referencing.cs.RangeMeaning;
import org.opengis.referencing.operation.CoordinateOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zahnen
 */
@Singleton
@AutoBind
public class CrsTransformerFactoryProj extends AbstractVolatile
    implements CrsTransformerFactory, CrsInfo, AppLifeCycle {

  private static final Logger LOGGER = LoggerFactory.getLogger(CrsTransformerFactoryProj.class);

  private final ProjLoader projLoader;
  private final ResourceStore projStore;
  private final Map<EpsgCrs, CoordinateReferenceSystem> crsCache;
  private final Map<EpsgCrs, Map<EpsgCrs, CrsTransformer>> transformerCache;
  private final Map<EpsgCrs, Map<EpsgCrs, CrsTransformer>> transformerCacheForce2d;
  private final boolean useCaches;
  private final boolean asyncStartup;

  @Inject
  public CrsTransformerFactoryProj(
      ProjLoader projLoader,
      ResourceStore resourceStore,
      VolatileRegistry volatileRegistry,
      AppContext appContext) {
    super(volatileRegistry, "app/crs");
    setHealthInfo(
        "PROJ CRS Transformations",
        "Might be LIMITED if store sources with custom PROJ configurations could not be loaded.\nMight be UNAVAILABLE if the native PROJ library could not be initialized.");

    this.projLoader = projLoader;
    this.projStore = resourceStore.with("proj");
    this.crsCache = new ConcurrentHashMap<>();
    this.transformerCache = new ConcurrentHashMap<>();
    this.transformerCacheForce2d = new ConcurrentHashMap<>();
    this.useCaches = true;
    this.asyncStartup = appContext.getConfiguration().getModules().isStartupAsync();
  }

  // for unit tests
  CrsTransformerFactoryProj(
      ProjLoader projLoader, ResourceStore resourceStore, VolatileRegistry volatileRegistry) {
    super(volatileRegistry);
    this.projLoader = projLoader;
    this.projStore = resourceStore.with("proj");
    this.crsCache = new ConcurrentHashMap<>();
    this.transformerCache = new ConcurrentHashMap<>();
    this.transformerCacheForce2d = new ConcurrentHashMap<>();
    this.useCaches = true;
    this.asyncStartup = false;
  }

  @Override
  public int getPriority() {
    return 100;
  }

  @Override
  public CompletionStage<Void> onStart(boolean isStartupAsync) {
    onVolatileStart();

    return getVolatileRegistry()
        .onAvailable(projStore)
        .thenRun(
            () -> {
              State state = State.AVAILABLE;

              try {
                projLoader.load();

                Optional<Path> customPath = Optional.empty();
                try {
                  customPath = projStore.asLocalPath(Path.of(""), false);
                } catch (IOException e) {
                  LogContext.error(LOGGER, e, "Could not initialize PROJ custom data directory");
                  state = State.LIMITED;
                  setMessage("Could not initialize PROJ custom data directory: " + e.getMessage());
                }

                String[] locations =
                    customPath.isPresent()
                        ? new String[] {
                          projLoader.getDataDirectory().toString(),
                          customPath.get().normalize().toString()
                        }
                        : new String[] {projLoader.getDataDirectory().toString()};

                Proj.setSearchPath(locations);
                Proj.version()
                    .ifPresent(
                        version ->
                            LOGGER.debug("PROJ version: {}, locations: {}", version, locations));
                System.setProperty("org.kortforsyningen.proj.maxThreadsPerInstance", "16");

                setState(state);
              } catch (Throwable e) {
                LogContext.error(LOGGER, e, "Could not initialize PROJ");
                setMessage("Could not initialize PROJ: " + e.getMessage());

                if (!asyncStartup) {
                  throw e;
                }
              }
            });
  }

  @Override
  public boolean isSupported(EpsgCrs crs) {
    return getCrs(crs).isPresent();
  }

  @Override
  public boolean is3d(EpsgCrs crs) {
    return getCrs(crs).filter(this::isCrs3d).isPresent();
  }

  @Override
  public Unit<?> getUnit(EpsgCrs crs) {
    return getHorizontalCrs(getCrsOrThrow(crs)).getCoordinateSystem().getAxis(0).getUnit();
  }

  @Override
  public List<String> getAxisAbbreviations(EpsgCrs crs) {
    CoordinateReferenceSystem projCrs = getCrsOrThrow(crs);

    return IntStream.range(0, is3d(crs) ? 3 : 2)
        .mapToObj(i -> projCrs.getCoordinateSystem().getAxis(i).getAbbreviation())
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public List<Unit<?>> getAxisUnits(EpsgCrs crs) {
    CoordinateReferenceSystem projCrs = getCrsOrThrow(crs);

    return IntStream.range(0, is3d(crs) ? 3 : 2)
        .mapToObj(i -> projCrs.getCoordinateSystem().getAxis(i).getUnit())
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public List<Optional<Double>> getAxisMinimums(EpsgCrs crs) {
    CoordinateReferenceSystem projCrs = getCrsOrThrow(crs);

    return IntStream.range(0, is3d(crs) ? 3 : 2)
        .mapToObj(i -> projCrs.getCoordinateSystem().getAxis(i).getMinimumValue())
        .map(Optional::ofNullable)
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public List<Optional<Double>> getAxisMaximums(EpsgCrs crs) {
    CoordinateReferenceSystem projCrs = getCrsOrThrow(crs);

    return IntStream.range(0, is3d(crs) ? 3 : 2)
        .mapToObj(i -> projCrs.getCoordinateSystem().getAxis(i).getMaximumValue())
        .map(Optional::ofNullable)
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public OptionalInt getAxisWithWraparound(EpsgCrs crs) {
    CoordinateReferenceSystem projCrs = getCrsOrThrow(crs);

    // first check, if the range meaning is provided (typically this is not the case)
    CoordinateSystem cs = projCrs.getCoordinateSystem();
    for (int i = 0; i < cs.getDimension(); i++)
      if (RangeMeaning.WRAPAROUND.equals(cs.getAxis(i).getRangeMeaning())) return OptionalInt.of(i);

    // otherwise we analyse the CRS definition

    // if we have a compound CRS, we analyse the coordinate system of the first CRS, which includes
    // the horizontal axes
    if (projCrs instanceof CompoundCRS)
      cs = ((CompoundCRS) projCrs).getComponents().get(0).getCoordinateSystem();

    // wraparound only occurs in ellipsoidal coordinate systems
    if (!(cs instanceof EllipsoidalCS)) return OptionalInt.empty();

    // find the longitude axis
    for (int i = 0; i < cs.getDimension(); i++)
      if (AxisDirection.EAST.equals(cs.getAxis(i).getDirection())
          || AxisDirection.WEST.equals(cs.getAxis(i).getDirection())) return OptionalInt.of(i);

    return OptionalInt.empty();
  }

  @Override
  public List<AxisDirection> getAxisDirections(EpsgCrs crs) {
    CoordinateReferenceSystem projCrs = getCrsOrThrow(crs);

    return IntStream.range(0, is3d(crs) ? 3 : 2)
        .mapToObj(i -> projCrs.getCoordinateSystem().getAxis(i).getDirection())
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public Optional<BoundingBox> getDomainOfValidity(EpsgCrs crs) {
    CoordinateReferenceSystem projCrs = getCrsOrThrow(crs);

    Extent extent = projCrs.getDomainOfValidity();
    if (Objects.isNull(extent)) return Optional.empty();

    Collection<? extends GeographicExtent> geographicElements = extent.getGeographicElements();
    if (Objects.isNull(geographicElements) || geographicElements.isEmpty()) return Optional.empty();

    List<BoundingBox> bboxs =
        geographicElements.stream()
            .map(
                geographicExtent -> {
                  if (geographicExtent instanceof GeographicBoundingBox) {
                    GeographicBoundingBox geoBbox = (GeographicBoundingBox) geographicExtent;
                    return BoundingBox.of(
                        geoBbox.getWestBoundLongitude(),
                        geoBbox.getSouthBoundLatitude(),
                        geoBbox.getEastBoundLongitude(),
                        geoBbox.getNorthBoundLatitude(),
                        OgcCrs.CRS84);
                  }
                  // TODO support also bounding polygons?
                  return null;
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList());
    if (bboxs.isEmpty()) return Optional.empty();
    else if (bboxs.size() == 1) return Optional.of(bboxs.get(0));

    // we have multiple bboxes, construct the bbox that includes them all;
    // also take care of bounding boxes that cross the antimeridian and normalize
    // the longitude values for the compuation so that west < east also for those
    // bounding boxes
    return bboxs.stream()
        .map(
            bbox ->
                bbox.getXmin() > bbox.getXmax()
                    ? BoundingBox.of(
                        bbox.getXmin() - 180,
                        bbox.getYmin(),
                        bbox.getXmax(),
                        bbox.getYmax(),
                        OgcCrs.CRS84)
                    : bbox)
        .map(BoundingBox::toArray)
        .reduce(
            (doubles, doubles2) ->
                new double[] {
                  Math.min(doubles[0], doubles2[0]),
                  Math.min(doubles[1], doubles2[1]),
                  Math.max(doubles[2], doubles2[2]),
                  Math.max(doubles[3], doubles2[3])
                })
        .map(
            doubles ->
                BoundingBox.of(
                    doubles[0] < -180 ? doubles[0] + 180 : doubles[0],
                    doubles[1],
                    doubles[2],
                    doubles[3],
                    OgcCrs.CRS84));
  }

  @Override
  public Optional<CrsTransformer> getTransformer(EpsgCrs sourceCrs, EpsgCrs targetCrs) {
    return getTransformer(sourceCrs, targetCrs, false);
  }

  @Override
  public Optional<CrsTransformer> getTransformer(
      EpsgCrs sourceCrs, EpsgCrs targetCrs, boolean force2d) {
    if (Objects.equals(sourceCrs, targetCrs)) {
      return Optional.empty();
    }
    CoordinateReferenceSystem sourceProjCrs = getCrsOrThrow(sourceCrs);
    CoordinateReferenceSystem targetProjCrs = getCrsOrThrow(targetCrs);

    if (force2d) {
      sourceProjCrs = getHorizontalCrs(sourceProjCrs);
      targetProjCrs = getHorizontalCrs(targetProjCrs);
    }

    CoordinateReferenceSystem finalSourceProjCrs = sourceProjCrs;
    CoordinateReferenceSystem finalTargetProjCrs = targetProjCrs;
    CrsTransformer transformer =
        useCaches
            ? getCacheForSource(sourceCrs, force2d)
                .computeIfAbsent(
                    targetCrs,
                    ignore ->
                        createCrsTransformer(
                            sourceCrs, targetCrs, finalSourceProjCrs, finalTargetProjCrs))
            : createCrsTransformer(sourceCrs, targetCrs, sourceProjCrs, targetProjCrs);

    return Optional.of(transformer);
  }

  @Override
  public List<Integer> getPrecisionList(EpsgCrs crs, Map<String, Integer> coordinatePrecision) {
    ImmutableList.Builder<Integer> precisionListBuilder = new ImmutableList.Builder<>();
    getAxisUnits(crs)
        .forEach(
            unit -> {
              if (unit.equals(Units.METRE)) {
                precisionListBuilder.add(
                    coordinatePrecision.getOrDefault(
                        "meter", coordinatePrecision.getOrDefault("metre", 0)));
              } else if (unit.equals(Units.DEGREE)) {
                precisionListBuilder.add(coordinatePrecision.getOrDefault("degree", 0));
              } else {
                if (LOGGER.isWarnEnabled()) {
                  LOGGER.warn(
                      "Coordinate precision could not be set, unrecognised unit found: '{}'.",
                      unit.getName());
                }
                precisionListBuilder.add(0);
              }
            });
    return precisionListBuilder.build();
  }

  private Optional<CoordinateReferenceSystem> getCrs(EpsgCrs crs) {
    if (Objects.isNull(crs)) {
      throw new IllegalArgumentException();
    }

    return useCaches
        ? Optional.ofNullable(crsCache.computeIfAbsent(crs, ignore -> createCrs(crs).orElse(null)))
        : createCrs(crs);
  }

  private CoordinateReferenceSystem getCrsOrThrow(EpsgCrs crs) {
    return getCrs(crs)
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        "CRS %s is not supported.",
                        Objects.nonNull(crs) ? crs.toSimpleString() : "null")));
  }

  private synchronized Optional<CoordinateReferenceSystem> createCrs(EpsgCrs crs) {
    return createCrs(crs, EPSG.provider(), Proj.getFactory(CRSFactory.class), false);
  }

  private synchronized Optional<CoordinateReferenceSystem> createCrs(
      EpsgCrs crs, CRSAuthorityFactory authorityFactory, CRSFactory crsFactory, boolean logError) {
    try {
      String code = String.valueOf(applyWorkarounds(crs).getCode());
      CoordinateReferenceSystem coordinateReferenceSystem =
          authorityFactory.createCoordinateReferenceSystem(code);
      coordinateReferenceSystem =
          applyAxisOrder(coordinateReferenceSystem, crs.getForceAxisOrder());

      if (crs.getVerticalCode().isPresent()) {
        String verticalCode = String.valueOf(crs.getVerticalCode().getAsInt());
        CoordinateReferenceSystem verticalCrs = authorityFactory.createVerticalCRS(verticalCode);
        CoordinateReferenceSystem compoundCrs =
            crsFactory.createCompoundCRS(
                ImmutableMap.of(
                    "name",
                    String.format(
                        "%s + %s", coordinateReferenceSystem.getName(), verticalCrs.getName())),
                coordinateReferenceSystem,
                verticalCrs);

        return Optional.of(compoundCrs);
      }
      return Optional.of(coordinateReferenceSystem);
    } catch (Throwable e) {
      if (logError) {
        LogContext.error(LOGGER, e, "PROJ");
      } else {
        LogContext.errorAsDebug(LOGGER, e, "PROJ");
      }
      return Optional.empty();
    }
  }

  private boolean isCrs3d(CoordinateReferenceSystem crs) {
    return crs.getCoordinateSystem().getDimension() == 3;
  }

  private EpsgCrs applyWorkarounds(EpsgCrs crs) {
    // ArcGIS still uses code 102100 instead of 3857, but proj does not support it anymore
    if (crs.getCode() == 102100) {
      return EpsgCrs.of(3857);
    }
    // FME uses code 900914 instead of 4979/CRS84h, but proj does not support this
    if (crs.getCode() == 900914) {
      return OgcCrs.CRS84h;
    }
    return crs;
  }

  private SingleCRS getHorizontalCrs(CoordinateReferenceSystem crs) {

    /* TODO: generic solution for GeographicCRS, this is from GeoTools
    final GeodeticDatum datum = ((GeographicCRS) crs).getDatum();
    Map<String, ?> properties = CRSUtilities.changeDimensionInName(cs, "3D", "2D");
    EllipsoidalCS horizontalCS;
    try {
        horizontalCS =
                ReferencingFactoryFinder.getCSFactory(null)
                        .createEllipsoidalCS(properties, axis0, axis1);
    } catch (FactoryException e) {
        Logging.recoverableException(CRS.class, "getHorizontalCRS", e);
        horizontalCS = new DefaultEllipsoidalCS(properties, axis0, axis1);
    }
    properties = CRSUtilities.changeDimensionInName(crs, "3D", "2D");
    GeographicCRS horizontalCRS;
    try {
        horizontalCRS =
                ReferencingFactoryFinder.getCRSFactory(null)
                        .createGeographicCRS(properties, datum, horizontalCS);
    } catch (FactoryException e) {
        Logging.recoverableException(CRS.class, "getHorizontalCRS", e);
        horizontalCRS = new DefaultGeographicCRS(properties, datum, horizontalCS);
    }
    return horizontalCRS;
     */

    // workaround for CRS84h and EPSG:4979
    if (crs instanceof GeographicCRS && crs.getCoordinateSystem().getDimension() == 3) {
      String name = crs.getName().getCode();
      if (name.startsWith("WGS 84")) {
        if (name.contains("normalized for visualization")) {
          return (SingleCRS) getCrsOrThrow(OgcCrs.CRS84);
        } else {
          return (SingleCRS) getCrsOrThrow(EpsgCrs.of(4326));
        }
      }
    }

    return crs instanceof CompoundCRS
        ? (SingleCRS) ((CompoundCRS) crs).getComponents().get(0)
        : (SingleCRS) crs;
  }

  private CoordinateReferenceSystem applyAxisOrder(
      CoordinateReferenceSystem crs, EpsgCrs.Force axisOrder) {
    if (axisOrder == EpsgCrs.Force.LON_LAT) {
      return Proj.normalizeForVisualization(crs);
    }

    return crs;
  }

  private Map<EpsgCrs, CrsTransformer> getCacheForSource(EpsgCrs crs, boolean force2d) {
    return force2d
        ? transformerCacheForce2d.computeIfAbsent(crs, ignore -> new ConcurrentHashMap<>())
        : transformerCache.computeIfAbsent(crs, ignore -> new ConcurrentHashMap<>());
  }

  private synchronized CrsTransformer createCrsTransformer(
      EpsgCrs sourceCrs,
      EpsgCrs targetCrs,
      CoordinateReferenceSystem sourceProjCrs,
      CoordinateReferenceSystem targetProjCrs) {

    boolean is3dTo3d = isCrs3d(sourceProjCrs) && isCrs3d(targetProjCrs);
    int sourceDimension = isCrs3d(sourceProjCrs) ? 3 : 2;
    int targetDimension = is3dTo3d ? 3 : 2;

    try {
      CoordinateOperationContext coordinateOperationContext = new CoordinateOperationContext();
      coordinateOperationContext.setAuthority("EPSG");
      coordinateOperationContext.setSpatialCriterion(SpatialCriterion.PARTIAL_INTERSECTION);
      coordinateOperationContext.setGridAvailabilityUse(
          GridAvailabilityUse.DISCARD_OPERATION_IF_MISSING_GRID);
      CoordinateOperation coordinateOperation =
          Proj.createCoordinateOperation(sourceProjCrs, targetProjCrs, coordinateOperationContext);

      checkForMissingGridFiles(coordinateOperation);

      LOGGER.debug(
          "Chosen operation for {} -> {}: {}{}",
          sourceCrs.toHumanReadableString(),
          targetCrs.toHumanReadableString(),
          coordinateOperation.getName().getCode(),
          getGridFile(coordinateOperation).orElse(""));

      CoordinateOperation horizontalCoordinateOperation = null;
      if (sourceDimension == 3) {
        horizontalCoordinateOperation =
            Proj.createCoordinateOperation(
                getHorizontalCrs(sourceProjCrs),
                getHorizontalCrs(targetProjCrs),
                coordinateOperationContext);
        LOGGER.debug(
            "Chosen operation for '{}' -> '{}': {}{}",
            getHorizontalCrs(sourceProjCrs).getName().getCode(),
            getHorizontalCrs(targetProjCrs).getName().getCode(),
            coordinateOperation.getName().getCode(),
            getGridFile(horizontalCoordinateOperation).orElse(""));
      }

      return new CrsTransformerProj(
          sourceProjCrs,
          targetProjCrs,
          sourceCrs,
          targetCrs,
          sourceDimension,
          targetDimension,
          coordinateOperation,
          Optional.ofNullable(horizontalCoordinateOperation));
    } catch (IllegalStateException ex) {
      // LogContext.error(LOGGER, ex, "PROJ");
      throw ex;
    } catch (Throwable ex) {
      LogContext.errorAsDebug(LOGGER, ex, "PROJ");
      throw new IllegalArgumentException(ex.getMessage(), ex);
    }
  }

  private void checkForMissingGridFiles(CoordinateOperation coordinateOperation) {
    try {
      double[] coordinates = new double[] {0, 0};
      coordinateOperation.getMathTransform().transform(coordinates, 0, coordinates, 0, 1);
    } catch (OutOfMemoryError e) {
      Pattern pattern = Pattern.compile("PARAMETERFILE\\[\"(.*?)\",\"(.*?)\"\\]");
      Matcher matcher = pattern.matcher(coordinateOperation.toWKT());
      if (matcher.find()) {
        throw new IllegalStateException(
            String.format(
                "Missing PROJ parameter file: %s (%s)", matcher.group(2), matcher.group(1)));
      }
      throw new IllegalStateException(e.getMessage());
    } catch (Throwable e) {
      // ignore
    }
  }

  private Optional<String> getGridFile(CoordinateOperation coordinateOperation) {

    Pattern pattern = Pattern.compile("PARAMETERFILE\\[\"(.*?)\",\"(.*?)\"\\]");
    Matcher matcher = pattern.matcher(coordinateOperation.toWKT());
    if (matcher.find()) {
      return Optional.of(
          String.format(" with parameter file '%s' (%s)", matcher.group(2), matcher.group(1)));
    }
    return Optional.empty();
  }
}
