'use strict'

const { MissingRequiredArgumentError, IllegalArgumentError } = require('@northscaler/error-support')
const uuid = require('uuid').v1
const { Trait } = require('@northscaler/mutrait')
const { UniqueKeyViolationError } = require('../errors')

const ERROR_CODES = {
  DUPLICATE_KEY: 11000
}

const MongoRepository = Trait(superclass =>
  class extends superclass {
    _client
    _db
    _collection
    _type // subclasses should set to class of entity being persisted, like: this._type = Customer

    _assert (entity) {
      if (!entity) throw new MissingRequiredArgumentError({ message: `${this._type.name} required` })
      if (!(entity instanceof this._type)) {
        throw new IllegalArgumentError({
          message: `type ${this._type.name} required`,
          info: { given: entity }
        })
      }
    }

    _removeNullishesIn (o) {
      const recurse = it => this._removeNullishesIn(it)

      if (o === null || o === undefined) return o
      else if (Array.isArray(o)) return o.map(recurse)
      else if (typeof o !== 'object') return o

      Object.keys(o).forEach(key => {
        if (o[key] === null || o[key] === undefined) delete o[key]
        else if (Array.isArray(o[key])) return o[key].map(recurse)
        else if (typeof o[key] === 'object') o[key] = this._removeNullishesIn(o[key])
      })

      return o
    }

    /**
     * Returns the given collection or `this._collection`
     * @private
     */
    _getCollection (collection) {
      return collection || this._collection
    }

    async _insert (object, { collection, options } = {}) {
      if (!object) throw new IllegalArgumentError({ info: { object } })
      object._id = object._id || uuid()

      await this._tryDbOp(async () => await this._getCollection(collection).insertOne(object, options))

      return object
    }

    async _upsert (object, { collection, options } = {}) {
      if (!object) throw new IllegalArgumentError({ info: { object } })
      object._id = object._id || uuid()

      await this._tryDbOp(async () => await this._getCollection(collection).updateOne(
        { _id: object._id },
        { $set: object },
        { ...options, upsert: true }
      ))

      return object
    }

    async _overwrite (object, { collection, options } = {}) {
      if (!object) throw new IllegalArgumentError({ info: { object } })
      object._id = object._id || uuid()

      await this._tryDbOp(async () => await this._getCollection(collection).replaceOne(
        { _id: object._id },
        object,
        options
      ))

      return object
    }

    /**
     * Returns the identified collection entry or `null` if not found.
     *
     * @param {*} id If not an `object`, the filter becomes `{ _id: id }`, else the filter is literally the object given.
     * @param {object} [collection] The mongodb collection to use; default is `this._collection`.
     * @param {object} [options] The mongodb `Collection#findOne` options.
     * @return {Promise<object|null>}
     * @see {@link MongoRepository#_getById}
     * @private
     */
    async _findById (id, { collection, options } = {}) {
      if (!id) throw new IllegalArgumentError({ info: { id } })

      return this._tryDbOp(async () =>
        this._getCollection(collection).findOne(typeof id === 'object' ? id : { _id: id }, options))
    }

    /**
     * Returns the identified collection entry or throws `ObjectNotFoundError` if not found.
     *
     * @param {*} id If not an `object`, the filter becomes `{ _id: id }`, else the filter is literally the object given.
     * @param {object} [collection] The mongodb collection to use; default is `this._collection`.
     * @param {object} [options] The mongodb `Collection#findOne` options.
     * @return {Promise<object|null>}
     * @throws ObjectNotFoundError If the identified entry does not exist.
     * @see {@link MongoRepository#_findById}
     * @private
     */
    async _getById (id, { collection, options } = {}) {
      return this._tryDbOp(async () => await this._findById(id, {
        collection,
        options
      }) || throw new IllegalArgumentError({
        message: 'not found',
        info: { id }
      }))
    }

    /**
     * Deletes the identified collection entry.
     *
     * @param {*} id If not an `object`, the filter becomes `{ _id: id }`, else the filter is literally the object given.
     * @param {object} [collection] The mongodb collection to use; default is `this._collection`.
     * @param {object} [options] The mongodb `Collection#deleteOne` options.
     * @return {Promise<object|null>}
     * @throws ObjectNotFoundError If the identified entry does not exist.
     * @see {@link MongoRepository#_findById}
     * @private
     */
    async _deleteById (id, { collection, options } = {}) {
      if (!id) throw new IllegalArgumentError({ info: { id } })

      await this._tryDbOp(async () => await this._getCollection(collection).deleteOne(typeof id === 'object' ? id : { _id: id }, options))
    }

    /**
     * Executes the given function within the scope of a transaction.
     * @param {Function} fn The function to execute; must return a `Promise`.
     * @param {Object} [options] Transaction options; see http://mongodb.github.io/node-mongodb-native/3.6/api/global.html#TransactionOptions for supported properties.
     * @return {Promise<*>} The promise returned by param `fn`.
     */
    async _transactionallyExecute (
      fn,
      options = {
        readPreference: 'primary',
        readConcern: { level: 'local' },
        writeConcern: { w: 'majority' }
      }
    ) {
      let session

      try {
        session = this._client.startSession()
        return session.withTransaction(fn, options)
      } finally {
        if (session) await session.endSession()
      }
    }

    async _tryDbOp (fn, collection) {
      try {
        return (await fn(collection))
      } catch (e) {
        throw this._translateError(e)
      }
    }

    _translateError (e, opts) {
      switch (e.code) {
        case ERROR_CODES.DUPLICATE_KEY:
          return new UniqueKeyViolationError({ ...opts, cause: e })
        default:
          return e
      }
    }
  }
)

module.exports = MongoRepository
