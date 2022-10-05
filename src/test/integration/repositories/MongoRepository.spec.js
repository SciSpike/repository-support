/* global describe, it, before */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect

const { mongoConnect } = require('@ballistagroup/mongo-test-support')
const uuid = require('uuid').v4
const { traits } = require('@ballistagroup/mutrait')
const { MongoRepository } = require('../../../main/repositories')
const { IllegalArgumentError } = require('@ballistagroup/error-support')
const { UniqueKeyViolationError, ObjectNotFoundError } = require('../../../main/errors')

class Repo extends traits(MongoRepository) {
  constructor ({ db, collection }) {
    super(...arguments)
    this._db = db
    this._collection = collection
  }
}

describe('integration tests of MongoRepository', () => {
  let db
  let collection
  let repo

  before(async function () {
    this.timeout(10000)

    db = await mongoConnect(process.env.CI_COMMIT_SHA ? { host: 'localhost', port: 27017 } : undefined)

    try {
      for (const c of await db.collections()) {
        await db.dropCollection(c.collectionName)
      }
    } catch (e) {}

    collection = await db.createCollection(uuid())
    repo = new Repo({ db, collection })
  })

  it('should insert a document', async function () {
    const doc = { _id: uuid(), _a: 1 }
    await repo._insert(doc)
    const it = await repo._getById(doc._id)
    expect(it).to.deep.equal(doc)

    try {
      await repo._insert(doc)
      expect.fail('should have thrown')
    } catch (e) {
      expect(e.name).to.equal('UniqueKeyViolationError')
      expect(e.code).to.equal(UniqueKeyViolationError.CODE)
      expect(e.cause.name).to.equal('MongoError')
      expect(e.cause.code).to.equal(11000)
    }
  })

  it('should insert a document with no _id', async function () {
    const doc = { _a: 1 }
    await repo._insert(doc)
    expect(doc._id).to.be.ok()
    const it = await repo._getById(doc._id)
    expect(it).to.deep.equal(doc)

    try {
      await repo._insert(doc)
      expect.fail('should have thrown')
    } catch (e) {
      expect(e.name).to.equal('UniqueKeyViolationError')
      expect(e.code).to.equal(UniqueKeyViolationError.CODE)
      expect(e.cause.name).to.equal('MongoError')
      expect(e.cause.code).to.equal(11000)
    }
  })

  it('should insert a document overriding collection', async function () {
    const doc = { _id: uuid(), _a: 1 }
    const opts = { collection: db.collection('bars') }
    await repo._insert(doc, opts)
    const it = await repo._getById(doc._id, opts)
    expect(it).to.deep.equal(doc)

    try {
      await repo._insert(doc, opts)
      expect.fail('should have thrown')
    } catch (e) {
      expect(e.name).to.equal('UniqueKeyViolationError')
      expect(e.code).to.equal(UniqueKeyViolationError.CODE)
      expect(e.cause.name).to.equal('MongoError')
      expect(e.cause.code).to.equal(11000)
    }
  })

  it('should upsert a document', async function () {
    const doc = { _id: uuid(), _a: 1 }
    await repo._upsert(doc)
    let it = await repo._getById(doc._id)
    expect(it).to.deep.equal(doc)

    doc._a = 2
    await repo._upsert(doc)
    it = await repo._getById(doc._id)
    expect(it).to.deep.equal(doc)
  })

  it('should update a document', async function () {
    const doc = { _id: uuid(), _a: 1 }
    console.log(doc._id)
    await repo._insert(doc)
    let it = await repo._getById(doc._id)
    expect(it).to.deep.equal(doc)

    doc._a = 2
    await repo._update(doc)
    it = await repo._getById(doc._id)
    expect(it).to.deep.equal(doc)

    try {
      doc._id = uuid()
      await repo._update(doc)
      expect.fail('should have thrown')
    } catch (e) {
      expect(e.name).to.equal('ObjectNotFoundError')
      expect(e.code).to.equal(ObjectNotFoundError.CODE)
      console.log(e)
    }
  })

  it('should overwrite a document', async function () {
    const doc = { _id: uuid(), _a: 1 }
    await repo._upsert(doc)
    let it = await repo._getById(doc._id)
    expect(it).to.deep.equal(doc)

    delete doc._a
    doc._b = 1

    await repo._overwrite(doc)
    it = await repo._getById(doc._id)
    expect(it).to.deep.equal(doc)
  })

  it('should be null on findById with unknown id', async function () {
    expect(await repo._findById(uuid())).to.be.null()
  })

  it('should throw on getById with unknown id', async function () {
    expect(
      async () => await repo._getById(uuid()).to.throw(IllegalArgumentError)
    )
  })

  it('should throw on no object', async function () {
    expect(async () => await repo._insert().to.throw(IllegalArgumentError))
  })

  it('should remove nullishes', function () {
    const o = { a: 'a', b: { c: undefined, d: { e: null } }, f: ['f', { g: null, h: 'h' }] }
    const actual = repo._removeNullishesIn(o)
    expect(actual).to.deep.equal({ a: 'a', b: { d: {} }, f: ['f', { h: 'h' }] })
  })
})
