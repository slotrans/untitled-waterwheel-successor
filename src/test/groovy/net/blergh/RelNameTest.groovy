package net.blergh

import spock.lang.Specification

class RelNameTest extends Specification{
    def "new-ing a RelName with ordinary names works"() {
        when:
        def myName = new RelName("some_schema", "some_table")

        then:
        myName.schemaName() == "some_schema"
        myName.tableName() == "some_table"
        myName.toFullName() == "some_schema.some_table"
    }

    def "building a RelName from a well-formed full name works"() {
        when:
        def myName = RelName.fromString("some_schema.some_table")

        then:
        myName.schemaName() == "some_schema"
        myName.tableName() == "some_table"
        myName.toFullName() == "some_schema.some_table"
    }

    def "new-ing a RelName with a dot in the SCHEMA name fails"() {
        when:
        def myName = new RelName("dotted.schema", "some_table")

        then:
        thrown(IllegalArgumentException)
    }

    def "new-ing a RelName with a dot in the TABLE name fails"() {
        when:
        def myName = new RelName("some_schema", "dotted.table")

        then:
        thrown(IllegalArgumentException)
    }

    def "new-ing a RelName with a dquote in the SCHEMA name fails"() {
        when:
        def myName = new RelName("\"quoted schema\"", "some_table")

        then:
        thrown(IllegalArgumentException)
    }

    def "new-ing a RelName with a dquote in the TABLE name fails"() {
        when:
        def myName = new RelName("some_schema", "\"quoted table\"")

        then:
        thrown(IllegalArgumentException)
    }

    def "attempting to build a 1-part name fails"() {
        when:
        def myName = RelName.fromString("foo")

        then:
        thrown(IllegalArgumentException)
    }

    def "attempting to build a 3-part name fails"() {
        when:
        def myName = RelName.fromString("foo.bar.baz")

        then:
        thrown(IllegalArgumentException)
    }
}
