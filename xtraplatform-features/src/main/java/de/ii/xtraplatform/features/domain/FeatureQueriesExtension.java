/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.github.azahnen.dagger.annotations.AutoMultiBind;
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
import java.util.function.BiConsumer;

/**
 * @langEn # Extensions
 *     <p>Extensions for Feature Providers add support for some uncommon use cases that are not
 *     covered by the core modules. The extensions are classified according to:
 *     <p><code>
 * - The state of the **implementation**
 *   - **mature**: no known limitations regarding generally supported use cases,
 *     adheres to code quality and testing standards
 *   - **candidate**: no known limitations regarding generally supported use cases,
 *      might not adhere to code quality and testing standards
 *   - **proposal**: stable core functionality, but might have limitations regarding generally
 *     supported use cases, might not adhere to code quality and testing standards
 *     </code>
 *     <p>## Overview
 *     <p>{@docTable:overview}
 * @langDe # Erweiterungen
 *     <p>Erweiterungen für Feature Provider unterstützen eher seltene Anwendungsfälle, die nicht
 *     Teil der Kernfunktionalität sind. Die Erweiterungen sind klassifiziert nach:
 *     <p><code>
 * - Dem Status der **Implementierung**
 *   - **mature**: keine Limitierungen bezogen auf allgemein unterstützte
 *     Anwendungsfälle, hält alle Code-Quality und Testing Standards ein
 *   - **candidate**: keine Limitierungen bezogen auf allgemein unterstützte Anwendungsfälle, hält eventuell nicht alle
 *     Code-Quality und Testing Standards ein
 *   - **proposal**: stabile Kernfunktionalität, aber kann
 *     Limitierungen bezogen auf allgemein unterstützte Anwendungsfälle enthalten, hält eventuell
 *     nicht alle Code-Quality und Testing Standards ein
 *     </code>
 *     <p>## Übersicht
 *     <p>{@docTable:overview}
 */
@DocFile(
    path = "providers/feature/90-extensions",
    name = "README.md",
    tables = {
      @DocTable(
          name = "overview",
          rows = {
            @DocStep(type = Step.IMPLEMENTATIONS),
            @DocStep(type = Step.SORTED, params = "{@title}")
          },
          columns = {
            @DocColumn(
                value = @DocStep(type = Step.TAG, params = "[{@title}]({@docFile:name})"),
                header = {
                  @DocI18n(language = "en", value = "Extension"),
                  @DocI18n(language = "de", value = "Erweiterung")
                }),
            @DocColumn(
                value =
                    @DocStep(
                        type = Step.TAG,
                        params =
                            "<SplitBadge type=\"{@module.maturity}\" left=\"impl\" right=\"{@module.maturity}\" vertical=\"center\" />"),
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
    })
@DocFilesTemplate(
    files = ForEach.IMPLEMENTATION,
    path = "providers/feature/90-extensions",
    // stripSuffix = "BuildingBlock",
    template = {
      @DocI18n(
          language = "en",
          value =
              "# {@title}\n\n"
                  + "<SplitBadge type=\"{@module.maturity}\" left=\"impl\" right=\"{@module.maturity}\" vertical=\"super\" />\n\n"
                  + "{@body}\n\n"
                  + "## Scope\n\n"
                  + "{@scopeEn |||}\n\n"
                  + "{@limitationsEn ### Limitations\n\n|||}\n\n"
                  + "## Configuration\n\n"
                  + "{@docTable:properties ### Options\n\n||| This extension has no configuration options.}\n\n"
                  + "{@docVar:example ### Example\n\n|||}\n"),
      @DocI18n(
          language = "de",
          value =
              "# {@title}\n\n"
                  + "<SplitBadge type=\"{@module.maturity}\" left=\"impl\" right=\"{@module.maturity}\" vertical=\"super\" />\n\n"
                  + "{@body}\n\n"
                  + "## Umfang\n\n"
                  + "{@scopeDe |||}\n\n"
                  + "{@limitationsDe ### Limitierungen\n\n|||}\n\n"
                  + "## Konfiguration\n\n"
                  + "{@docTable:properties ### Optionen\n\n||| Diese Erweiterung hat keine Konfigurationsoptionen.}\n\n"
                  + "{@docVar:example ### Beispiel\n\n|||}\n")
    },
    tables = {
      @DocTable(
          name = "properties",
          rows = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:propertyTable}"),
            @DocStep(type = Step.JSON_PROPERTIES),
            @DocStep(type = Step.UNMARKED)
          },
          columnSet = ColumnSet.JSON_PROPERTIES),
    },
    vars = {
      @DocVar(
          name = "example",
          value = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:example}"),
            @DocStep(type = Step.TAG, params = "{@bodyBlock}")
          })
    })
@AutoMultiBind
public interface FeatureQueriesExtension {

  enum LIFECYCLE_HOOK {
    STARTED
  }

  enum QUERY_HOOK {
    BEFORE,
    AFTER
  }

  boolean isSupported(FeatureProviderConnector<?, ?, ?> connector, FeatureProviderDataV2 data);

  void on(
      LIFECYCLE_HOOK hook,
      FeatureProviderEntity provider,
      FeatureProviderConnector<?, ?, ?> connector);

  void on(
      QUERY_HOOK hook,
      FeatureProviderDataV2 data,
      FeatureProviderConnector<?, ?, ?> connector,
      Query query,
      BiConsumer<String, String> aliasResolver);
}
