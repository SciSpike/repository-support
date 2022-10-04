'use strict'

const _ = {
  cloneDeepWith: require('lodash.clonedeepwith')
}
const { Trait } = require('@ballistagroup/mutrait')
const uuid = require('uuid').v4
const { MethodNotImplementedError } = require('@ballistagroup/error-support')
const { ObjectNotFoundError, ObjectExistsError } = require('../errors')
const { Enumeration } = require('@ballistagroup/enum-support')
const { IllegalArgumentError } = require('@ballistagroup/error-support')

const MongooseRepository = Trait(superclass =>
  class extends superclass {
    _db
    _model

    constructor (...args) {
      super(...args)
      this._mapperCache = {}
    }

    _initMongooseRepository (db, model) {
      this._db = db
      this._model = model
    }

    _getMapper (mapper, name) {
      name = name || mapper.name
      return (
        this._mapperCache[name] ||
        (this._mapperCache[name] = ({ key, from, getterPrefix }) => {
          const value = from[`${getterPrefix}${key}`]
          if (value === undefined || value === null) return value
          return Array.isArray(value) ? [...value.map(mapper)] : mapper(value)
        })
      )
    }

    _noOpMapper () {
      return this._getMapper(it => it, 'noop')
    }

    _toStringMapper () {
      return this._getMapper(it => it?.toString(), 'string')
    }

    _toFloatMapper () {
      return this._getMapper(it => parseFloat(it), 'float')
    }

    _toIntMapper () {
      return this._getMapper(it => parseInt(it), 'int')
    }

    _toBooleanMapper () {
      return this._getMapper(it => Boolean(it), 'boolean')
    }

    _toEnumMapper ({ enumeration }) {
      return this._getMapper(enumeration.of, `enum:${enumeration.name}`)
    }

    async insert (entity, options) {
      if (await this.findById(entity.id)) { throw new ObjectExistsError({ info: { entity } }) }
      return this.upsert(entity, options)
    }

    async upsert (entity, options) {
      if (!entity._id) entity._id = uuid()
      return this._model
        .findByIdAndUpdate(
          entity._id,
          this._toDocument(entity),
          options || {
            upsert: true,
            overwrite: true
          }
        )
        .exec()
    }

    async exists (entity) {
      return this.idExists(entity?.id)
    }

    async idExists (id) {
      return !!(await this.findById(id))
    }

    async findById (id) {
      if (!id) return null

      const doc = await this._model.findById(id).exec()
      return (
        doc &&
        this._fromDocument({
          plain: doc.toObject(),
          setterPrefix: '_',
          getterPrefix: '_'
        })
      )
    }

    async getById (id) {
      return (await this.findById(id)) || throw new ObjectNotFoundError({ info: { id } })
    }

    /**
     * The customizer function to use when {@link _toTree} calls lodash's `cloneDeepWith` function.
     * Note that if you're not converting a value, then return <code>undefined</code>.
     *
     * This default customizer returns a function that simply returns <code>undefined</code>.
     *
     * If you need more sophisticated behavior, either override this method or override {@link _toTree}.
     *
     * @returns {*}
     * @private
     */
    get _toTreeCustomizer () {
      return () => undefined
      // remember: only return something if you're converting it!
      // see https://github.com/lodash/lodash/issues/2846
    }

    /**
     * Converts the given entity graph into a tree structure.
     * This method is called by {@link _toDocument}.
     *
     * Subclasses must override this if the root entity of this repository type forms a graph.
     * If the root entity only forms a tree, this method can probably be used.
     * This default implementation uses {@link _toTreeCustomizer}.
     *
     * @param entity
     * @returns {any}
     * @private
     * @see _toTreeCustomizer
     * @see _toDocument
     */
    _toTree (entity) {
      return _.cloneDeepWith(entity, this._toTreeCustomizer)
    }

    /**
     * Converts the given entity into a plain, JavaScript object suitable for persistence into MongoDB.
     *
     * @param it
     * @return {*}
     */
    _toMongoDocument (it) {
      if (typeof it === 'function') {
        throw new IllegalArgumentError({
          message: 'functions cannot be converted to a mongo document'
        })
      }
      if (Array.isArray(it)) return it.map(it => this._toMongoDocument(it))
      if (Enumeration.isInstance(it)) return it.name

      if (typeof it === 'object') {
        if (it === null) return it

        return Object.keys(it)
          .filter(k => typeof it[k] !== 'function' && it[k] !== undefined)
          .map(k => ({ [k]: this._toMongoDocument(it[k]) }))
          .reduce((accum, next) => Object.assign(accum, next), {})
      }

      return it
    }

    /**
     * Extracts the persistable state of the given entity into a plain JavaScript tree structure, representing the document that will be stored.
     * This method calls {@link _toTree} before extracting the persistable state.
     *
     * @param entity
     * @returns {*}
     * @private
     * @see _toTree
     */
    _toDocument (entity) {
      return this._toMongoDocument(this._toTree(entity))
    }

    _fromDocument ({ plain, entity, context = {}, setterPrefix = '' } = {}) {
      throw new MethodNotImplementedError({ message: 'MongooseRepository#_fromDocument' })
    }

    // /**
    //  * Maps top-level properties (nonrecursively) from one object to another, optionally with a single custom mapping function or mapping functions by property name.
    //  *
    //  * @param {string|[string]} [keys] Optional key or keys to map; defaults to `Object.keys(from)`.
    //  * @param {*} [from] Optional object from which to map properties; defaults to `{}`.
    //  * @param {*} [to] Optional object to which to map properties; defaults to `{}`.
    //  * @param {string} [setterPrefix] Optional property setter prefix to use when setting properties on {@param to}; defaults to the empty string (`''`).
    //  * @param {string} [getterPrefix] Optional property getter prefix to use when getting properties from {@param from}; defaults to the empty string (`''`).
    //  * @param {function|{ string: function }} [mappers] Optional property conversion function, or functions by property name; defaults to the identity mapping with array copying if the source property is an array.
    //  * @return {*} The object mapped {@param to}.
    //  * @private
    //  */
    _mapProps ({ keys, from, to, setterPrefix, getterPrefix, mappers } = {}) {
      from = from || {}
      to = to || {}
      setterPrefix = setterPrefix || ''
      getterPrefix = getterPrefix || ''

      keys = keys || Object.keys(from)
      if (!Array.isArray(keys)) keys = [keys]

      return keys.reduce((accum, key) => {
        let map = (mappers && mappers[key]) || mappers

        let e
        if (Enumeration.isClass(map)) {
          // then map is a reference to an enum class
          e = map
          map = this._toEnumMapper({ key, from, enumeration: e })
        } else if (typeof map !== 'function') {
          // then map's either undefined or an object of mappers that doesn't apply to given key, so use default
          map = this._noOpMapper({ key, from })
        }

        const value = map({ key, from, getterPrefix, enumeration: e, to })
        if (value !== undefined) to[`${setterPrefix}${key}`] = value

        return accum
      }, to)
    }

    /**
     * Returns the given name prefixed by the given prefix.
     *
     * @param {string} name
     * @param {string} prefix
     * @return {string}
     * @private
     */
    _prop (name, prefix = '') {
      return `${prefix}${name}`
    }

    get _setOptions () {
      return DEFAULT_SET_OPTIONS
    }

    _translateError (e) {
      return e // TODO: translate exception into datastore-agnostic error
    }

    _trySync (it) {
      try {
        return it()
      } catch (x) {
        throw this._translateError(x)
      }
    }

    async _tryAsync (it) {
      try {
        return await it()
      } catch (x) {
        throw this._translateError(x)
      }
    }
  }
)

const DEFAULT_SET_OPTIONS = (MongooseRepository.DEFAULT_SET_OPTIONS = Object.freeze({ merge: true }))

module.exports = MongooseRepository
