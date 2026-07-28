/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.base.domain.resiliency.OptionalVolatileCapability;
import de.ii.xtraplatform.base.domain.resiliency.VolatileComposed;
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistered;
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry;
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry.ChangeHandler;
import de.ii.xtraplatform.docs.DocColumn;
import de.ii.xtraplatform.docs.DocFile;
import de.ii.xtraplatform.docs.DocFilesTemplate;
import de.ii.xtraplatform.docs.DocFilesTemplate.ForEach;
import de.ii.xtraplatform.docs.DocI18n;
import de.ii.xtraplatform.docs.DocStep;
import de.ii.xtraplatform.docs.DocStep.Step;
import de.ii.xtraplatform.docs.DocTable;
import de.ii.xtraplatform.docs.DocTable.ColumnSet;
import de.ii.xtraplatform.docs.DocVar;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * @langEn # Features
 *     <p>These types of feature providers are supported:
 *     <p>{@docTable:overview}
 *     <p>## Configuration
 *     <p>These are common configuration options for all provider types.
 *     <p>{@docTable:properties}
 *     <p>### Schema Definitions
 *     <p>{@docTable:types}
 *     <p>
 * @langDe # Allgemein
 *     <p>Diese Arten von Feature-Providern werden unterstützt:
 *     <p>{@docTable:overview}
 *     <p>## Konfiguration
 *     <p>Dies sind gemeinsame Konfigurations-Optionen für alle Provider-Typen.
 *     <p>{@docTable:properties}
 *     <p>### Schema-Definitionen
 *     <p>{@docTable:types}
 *     <p>
 * @langEn ### Connection Info
 *     <p>For data source specifics, see [SQL](10-sql.md#connection-info) and
 *     [WFS](50-wfs.md#connection-info).
 * @langDe ### Connection Info
 *     <p>Informationen zu den Datenquellen finden Sie auf separaten Seiten:
 *     [SQL](10-sql.md#connection-info) und [WFS](50-wfs.md#connection-info).
 *     <p>
 * @langEn ### Example Configuration (SQL)
 *     <p>See the [feature
 *     provider](https://github.com/interactive-instruments/ldproxy/blob/master/demo/vineyards/store/entities/providers/vineyards.yml)
 *     of the API [Vineyards in Rhineland-Palatinate, Germany](https://demo.ldproxy.net/vineyards).
 * @langDe ### Beispiel-Konfiguration (SQL)
 *     <p>Als Beispiel siehe die
 *     [Provider-Konfiguration](https://github.com/interactive-instruments/ldproxy/blob/master/demo/vineyards/store/entities/providers/vineyards.yml)
 *     der API [Weinlagen in Rheinland-Pfalz](https://demo.ldproxy.net/vineyards).
 * @langEn ### Mapping Operations
 *     <p>{@docVar:mappingOps}
 * @langDe ### Mapping Operationen
 *     <p>{@docVar:mappingOps}
 * @langEn ### Feature References
 *     <p>{@docVar:featureRefs}
 * @langDe ### Objektreferenzen
 *     <p>{@docVar:featureRefs}
 * @langEn ### Embedding Feature References
 *     <p>{@docVar:embedFeatureRefs}
 * @langDe ### Einbetten von Objektreferenzen
 *     <p>{@docVar:embedFeatureRefs}
 * @langEn ### Custom CQL2 Functions
 *     <p>{@docVar:cql2Functions}
 * @langDe ### Benutzerdefinierte CQL2-Funktionen
 *     <p>{@docVar:cql2Functions}
 * @ref:cfgProperties {@link de.ii.xtraplatform.features.domain.ImmutableFeatureProviderCommonData}
 * @ref:cfgProperties:types {@link de.ii.xtraplatform.features.domain.ImmutableFeatureSchema}
 * @ref:mappingOps {@link de.ii.xtraplatform.features.domain.MappingOperationResolver}
 * @ref:featureRefs {@link de.ii.xtraplatform.features.domain.transform.FeatureRefResolver}
 * @ref:embedFeatureRefs {@link de.ii.xtraplatform.features.domain.transform.FeatureRefEmbedder}
 * @ref:cql2Functions {@link de.ii.xtraplatform.cql.domain.CustomFunction}
 */
@DocFile(
    path = "providers/feature",
    name = "README.md",
    tables = {
      @DocTable(
          name = "properties",
          rows = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:cfgProperties}"),
            @DocStep(type = Step.JSON_PROPERTIES)
          },
          columnSet = ColumnSet.JSON_PROPERTIES),
      @DocTable(
          name = "types",
          rows = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:cfgProperties:types}"),
            @DocStep(type = Step.JSON_PROPERTIES)
          },
          columnSet = ColumnSet.JSON_PROPERTIES),
      @DocTable(
          name = "overview",
          rows = {
            @DocStep(type = Step.IMPLEMENTATIONS),
            @DocStep(type = Step.SORTED, params = "{@sortPriority}")
          },
          columns = {
            @DocColumn(
                value = @DocStep(type = Step.TAG, params = "[{@title}]({@docFile:name})"),
                header = {
                  @DocI18n(language = "en", value = "Provider Type"),
                  @DocI18n(language = "de", value = "Provider-Typ")
                }),
            @DocColumn(
                value =
                    @DocStep(
                        type = Step.TAG,
                        params =
                            "<SplitBadge type=\"{@module.maturity}\" left=\"impl\" right=\"{@module.maturity}\" vertical=\"center\" style=\"margin-bottom: 5px; margin-right: 5px;\" /><span/><SplitBadge type=\"{@module.maintenanceBadge}\" left=\"main\" right=\"{@module.maintenance}\" vertical=\"center\" />"),
                header = {
                  @DocI18n(language = "en", value = "Classification"),
                  @DocI18n(language = "de", value = "Klassifizierung")
                }),
            @DocColumn(
                value = @DocStep(type = Step.TAG, params = "{@body}"),
                header = {
                  @DocI18n(language = "en", value = "Description"),
                  @DocI18n(language = "de", value = "Beschreibung")
                })
          })
    },
    vars = {
      @DocVar(
          name = "mappingOps",
          value = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:mappingOps}"),
            @DocStep(type = Step.TAG, params = "{@bodyBlock}")
          }),
      @DocVar(
          name = "featureRefs",
          value = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:featureRefs}"),
            @DocStep(type = Step.TAG, params = "{@bodyBlock}")
          }),
      @DocVar(
          name = "embedFeatureRefs",
          value = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:embedFeatureRefs}"),
            @DocStep(type = Step.TAG, params = "{@bodyBlock}")
          }),
      @DocVar(
          name = "cql2Functions",
          value = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:cql2Functions}"),
            @DocStep(type = Step.TAG, params = "{@bodyBlock}")
          }),
    })
@DocFilesTemplate(
    files = ForEach.IMPLEMENTATION,
    path = "providers/feature",
    stripPrefix = "FeatureProvider",
    template = {
      @DocI18n(
          language = "en",
          value =
              "# {@title}\n\n"
                  + "<SplitBadge type=\"{@module.maturity}\" left=\"impl\" right=\"{@module.maturity}\" vertical=\"super\" />"
                  + "<SplitBadge type=\"{@module.maintenanceBadge}\" left=\"main\" right=\"{@module.maintenance}\" vertical=\"super\" />\n\n"
                  + "{@body}\n\n"
                  + "{@prerequisitesEn ## Prerequisites\n\n|||}\n\n"
                  + "{@limitationsEn ## Limitations\n\n|||}\n\n"
                  + "## Configuration\n\n"
                  + "{@docVar:cfgBody |||}\n\n"
                  + "{@docTable:cfgProperties ### Options\n\n||| This provider has no specific configuration options.}\n\n"
                  + "{@cfgPropertiesAdditionalEn |||}\n\n"
                  + "{@docVar:cfgExamples ### Examples\n\n|||}\n"),
      @DocI18n(
          language = "de",
          value =
              "# {@title}\n\n"
                  + "<SplitBadge type=\"{@module.maturity}\" left=\"impl\" right=\"{@module.maturity}\" vertical=\"super\" />"
                  + "<SplitBadge type=\"{@module.maintenanceBadge}\" left=\"main\" right=\"{@module.maintenance}\" vertical=\"super\" />\n\n"
                  + "{@body}\n\n"
                  + "{@prerequisitesDe ## Vorraussetzungen\n\n|||}\n\n"
                  + "{@limitationsDe ## Limitierungen\n\n|||}\n\n"
                  + "## Konfiguration\n\n"
                  + "{@docVar:cfgBody |||}\n\n"
                  + "{@docTable:cfgProperties ### Optionen\n\n||| Dieser Provider hat keine spezifischen Konfigurationsoptionen.}\n\n"
                  + "{@cfgPropertiesAdditionalDe |||}\n\n"
                  + "{@docVar:cfgExamples ### Beispiele\n\n|||}\n")
    },
    tables = {
      @DocTable(
          name = "cfgProperties",
          rows = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:cfgProperties}"),
            @DocStep(type = Step.JSON_PROPERTIES),
            @DocStep(type = Step.MARKED, params = "specific")
          },
          columnSet = ColumnSet.JSON_PROPERTIES),
    },
    vars = {
      @DocVar(
          name = "cfgBody",
          value = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:cfg}"),
            @DocStep(type = Step.TAG, params = "{@bodyBlock}")
          }),
      @DocVar(
          name = "cfgExamples",
          value = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:cfg}"),
            @DocStep(type = Step.TAG, params = "{@examples}")
          }),
    })
public interface FeatureProvider extends VolatileComposed {

  String PROVIDER_TYPE = "FEATURE";

  default String getId() {
    return info().getId();
  }

  default FeatureInfo info() {
    return (FeatureInfo) this;
  }

  FeatureChanges changes();

  default OptionalVolatileCapability<FeatureQueries> queries() {
    return new FeatureVolatileCapability<>(FeatureQueries.class, FeatureQueries.CAPABILITY, this);
  }

  default OptionalVolatileCapability<FeatureExtents> extents() {
    return new FeatureVolatileCapability<>(FeatureExtents.class, FeatureExtents.CAPABILITY, this);
  }

  default OptionalVolatileCapability<FeatureQueriesPassThrough> passThrough() {
    return new FeatureVolatileCapability<>(
        FeatureQueriesPassThrough.class, FeatureQueriesPassThrough.CAPABILITY, this);
  }

  default boolean supportsMutationsInternal() {
    return false;
  }

  default OptionalVolatileCapability<FeatureTransactions> mutations() {
    return new FeatureVolatileCapability<>(
        FeatureTransactions.class,
        FeatureTransactions.CAPABILITY,
        this,
        this::supportsMutationsInternal);
  }

  default OptionalVolatileCapability<FeatureCrs> crs() {
    return new FeatureVolatileCapability<>(
        FeatureCrs.class, FeatureCrs.CAPABILITY, this, info().getCrs()::isPresent);
  }

  default OptionalVolatileCapability<FeatureMetadata> metadata() {
    return new FeatureVolatileCapability<>(FeatureMetadata.class, FeatureMetadata.CAPABILITY, this);
  }

  default OptionalVolatileCapability<MultiFeatureQueries> multiQueries() {
    return new FeatureVolatileCapability<>(
        MultiFeatureQueries.class, MultiFeatureQueries.CAPABILITY, this);
  }

  // TODO: to QueryCapabilities
  default boolean supportsSorting() {
    return false;
  }

  // TODO: to QueryCapabilities
  default boolean supportsHighLoad() {
    return false;
  }

  // TODO: to QueryCapabilities
  default boolean supportsHitsOnly() {
    return false;
  }

  default String getCapabilityKey(String subKey) {
    return String.format("%s/%s", getUniqueKey(), subKey);
  }

  class FeatureVolatileCapability<T> implements OptionalVolatileCapability<T>, VolatileRegistered {

    private final Class<T> clazz;
    private final T obj;
    private final String key;
    private final VolatileComposed composed;
    private final Supplier<Boolean> onlyIf;

    public FeatureVolatileCapability(Class<T> clazz, String key, VolatileComposed composed) {
      this(clazz, null, key, composed, null);
    }

    public FeatureVolatileCapability(T obj, String key, VolatileComposed composed) {
      this((Class<T>) obj.getClass(), obj, key, composed, null);
    }

    public FeatureVolatileCapability(
        Class<T> clazz, String key, VolatileComposed composed, Supplier<Boolean> onlyIf) {
      this(clazz, null, key, composed, onlyIf);
    }

    public FeatureVolatileCapability(
        Class<T> clazz, T obj, String key, VolatileComposed composed, Supplier<Boolean> onlyIf) {
      this.clazz = clazz;
      this.obj = obj;
      this.key = key;
      this.composed = composed;
      this.onlyIf = onlyIf;
    }

    @Override
    public String getUniqueKey() {
      return String.format("%s/%s", composed.getUniqueKey(), key);
    }

    @Override
    public State getState() {
      return composed.getState(key);
    }

    @Override
    public Optional<String> getMessage() {
      return composed.getMessage(key);
    }

    @Override
    public Runnable onStateChange(ChangeHandler handler, boolean initialCall) {
      return composed.onStateChange(key, handler, initialCall);
    }

    @Override
    public boolean isSupported() {
      return (Objects.nonNull(obj) || clazz.isAssignableFrom(composed.getClass()))
          && (Objects.isNull(onlyIf) || onlyIf.get());
    }

    @Override
    public boolean isAvailable() {
      return isSupported() && OptionalVolatileCapability.super.isAvailable();
    }

    @Override
    public T get() {
      if (!isSupported()) {
        throw new UnsupportedOperationException(key + " not supported");
      }
      return Objects.nonNull(obj) ? obj : clazz.cast(composed);
    }

    @Override
    public VolatileRegistry getVolatileRegistry() {
      return ((VolatileRegistered) composed).getVolatileRegistry();
    }
  }
}
