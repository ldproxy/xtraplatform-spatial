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

    def "values in enum are an enumeration with the value as label"() {
        when:
        Codelist codelist = read('''\
label: Gebäudefunktion
enum:
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
enum:
  - a
  - b: c
''')

        then:
        thrown(MismatchedInputException)
    }

    def "a list in entries is rejected"() {
        when:
        read('''\
entries:
  - a
  - b
''')

        then:
        thrown(MismatchedInputException)
    }

    def "enum and entries are combined"(String payload) {
        when:
        Codelist codelist = read(payload)

        then:
        codelist.getEntries() == ['a': 'A', 'b': 'b']
        codelist.getEnumeration().isEmpty()
        !yaml.valueToTree(codelist).has('enum')

        where:
        payload << [
            'entries:\n  a: A\nenum:\n  - b\n',
            'enum:\n  - b\nentries:\n  a: A\n'
        ]
    }

    def "a value in enum that is also a key in entries keeps the label from entries"() {
        when:
        Codelist codelist = read('''\
entries:
  a: A
enum:
  - a
  - b
  - b
''')

        then:
        codelist.getEntries() == ['a': 'A', 'b': 'b']
    }
}
