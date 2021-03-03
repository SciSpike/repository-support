'use strict'

const os = require('os')
const { traits } = require('@northscaler/mutrait')
const { SchemaVersion } = require('../entities')
const MongoRepository = require('./MongoRepository')
const { ObjectNotFoundError } = require('../errors')
const { MissingRequiredArgumentError } = require('@northscaler/error-support')
const pkg = require('../../../package.json')

class MongoSchemaVersionRepository extends traits(MongoRepository) {
  static DEFAULT_COLLECTION_NAME = 'schema_versions'
  static DEFAULT_LOCK = MongoSchemaVersionRepository.formatLock(pkg)

  static formatLock (pkg, hostname = os.hostname()) {
    return `${pkg.name}@${pkg.version}@${hostname}`
  }

  static async ensureSchema ({ db, name = MongoSchemaVersionRepository.DEFAULT_COLLECTION_NAME, options }) {
    if (!db) throw MissingRequiredArgumentError({ message: 'db required' })
    if (!name) throw MissingRequiredArgumentError({ message: 'name required' })

    const collection = ((await db.collections()).map(it => it.collectionName).includes(name))
      ? db.collection(name)
      : await db.createCollection(name, options)

    return collection
  }

  _type = SchemaVersion

  constructor (collection) {
    super(...arguments)
    this._collection = collection || throw MissingRequiredArgumentError({ message: 'collection required' })
  }

  /**
   * Converts the given SchemaVersion into a document suitable for storage in `this._collection`.
   */
  _entityToDoc (entity) {
    const doc = this._removeNullishesIn({
      _id: entity._id,
      _semver: entity._semver,
      _locked: entity._locked
    })

    return doc
  }

  /**
   * Converts the given Mongo document back into a Fulfiller instance.
   */
  _docToEntity (doc, parent) {
    if (!doc) return doc

    const it = Object.create(SchemaVersion.prototype) // bypasses constructor
    it._id = doc._id
    it._semver = doc._semver
    it._locked = doc._locked

    return it
  }

  async upsert (schemaVersion, { options } = {}) {
    this._assert(schemaVersion)
    await this._upsert(this._entityToDoc(schemaVersion), { options })
    return schemaVersion
  }

  async findById ({ id }) {
    return this._docToEntity(await this._collection.findOne({ _id: id }))
  }

  async getById ({ id }) {
    return (await this.findById(arguments[0])) || throw new ObjectNotFoundError({
      message: 'schemaVersion not found',
      info: { id }
    })
  }
}

module.exports = MongoSchemaVersionRepository
