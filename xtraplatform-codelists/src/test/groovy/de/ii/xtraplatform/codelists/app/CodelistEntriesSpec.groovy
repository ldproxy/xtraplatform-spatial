/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.codelists.app

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import de.ii.xtraplatform.codelists.domain.Codelist
import de.ii.xtraplatform.codelists.domain.ImmutableCodelist
import spock.lang.Shared
import spock.lang.Specification

class CodelistEntriesSpec extends Specification {

    @Shared
    ObjectMapper yaml = new ObjectMapper(new YAMLFactory()).registerModule(new Jdk8Module())

    Codelist read(String payload) {
        ImmutableCodelist.Builder builder = new ImmutableCodelist.Builder()
        yaml.readerForUpdating(builder).readValue(payload)
        return builder.build()
    }

    def "entries as a map from code to label"() {
        when:
        Codelist codelist = read('''\
label: Cable Type
entries:
  10: No Tethering
  11: Load Bearing
''')

        then:
        codelist.getEntries() == ['10': 'No Tethering', '11': 'Load Bearing']
        codelist.getValue('11') == 'Load Bearing'
    }

    def "entries as a list of values are an enumeration with the value as label"() {
        when:
        Codelist codelist = read('''\
label: Gebäudefunktion
entries:
  - Wohngebäude
  - Bürogebäude
  - 1000
''')

        then:
        codelist.getEntries() == ['Wohngebäude': 'Wohngebäude', 'Bürogebäude': 'Bürogebäude', '1000': '1000']
        codelist.getEntries().keySet() as List == ['Wohngebäude', 'Bürogebäude', '1000']
        codelist.getValue('Bürogebäude') == 'Bürogebäude'
    }

    def "an enumeration with a nested value is rejected"() {
        when:
        read('''\
entries:
  - a
  - b: c
''')

        then:
        thrown(MismatchedInputException)
    }
}
