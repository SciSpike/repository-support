'use strict'

const { traits } = require('@northscaler/mutrait')
const { IllegalArgumentError } = require('@northscaler/error-support')
const { Identifiable, SemanticallyVersionable } = require('@northscaler/entity-support').traits
const property = require('@northscaler/property-decorator')

class SchemaVersion extends traits(Identifiable, SemanticallyVersionable) {
  constructor ({ id, semver }) {
    super(...arguments)

    this.id = id
    this.semver = semver
  }

  @property()
  _locked

  _testSetId (id) {
    if (typeof id !== 'string') throw new IllegalArgumentError({ message: 'type string required', info: { name: id } })
    if (!(id = id.trim())) { throw new IllegalArgumentError({ message: 'id required' }) }

    return id
  }

  gt (semver) {
    return this._semverGt(semver)
  }

  lt (semver) {
    return this._semverLt(semver)
  }

  gte (semver) {
    return this._semverGte(semver)
  }

  lte (semver) {
    return this._semverLte(semver)
  }
}

module.exports = SchemaVersion
