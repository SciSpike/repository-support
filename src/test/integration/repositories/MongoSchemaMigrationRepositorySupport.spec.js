/* global describe, it, before, beforeEach */
'use strict'

const path = require('path')
const chai = require('chai')
chai.use(require('chai-as-promised'))
chai.use(require('dirty-chai'))
const expect = chai.expect
const mockfs = require('mock-fs')
const mockRequire = require('mock-require')

const { mongoConnect, dropCollections } = require('@ballistagroup/mongo-test-support')
const uuid = require('uuid').v4
const { traits } = require('@ballistagroup/mutrait')
const { MongoRepository, MongoSchemaVersionRepository } = require('../../../main/repositories')
const { MongoSchemaMigrationRepositorySupport } = require('../../../main/traits')
const { SchemaVersion } = require('../../../main/entities')
const semver = require('semver')
const pkg = require('../../../../package.json')
const Promise = require('bluebird')

class Repo extends traits(MongoRepository, MongoSchemaMigrationRepositorySupport) {
  static DEFAULT_COLLECTION_NAME = 'test_collection'
  static SCHEMA_VERSION_ID = 'TestRepo'

  constructor ({ db, collection }) {
    super(...arguments)
    this._db = db
    this._collection = collection
  }

  static async ensureSchema ({
    db,
    name = Repo.DEFAULT_COLLECTION_NAME,
    options,
    schemaVersionRepository
  }) {
    return await this._ensureSchema({
      db,
      name: name,
      schemaVersionId: Repo.SCHEMA_VERSION_ID,
      pkg,
      migrationsDir: path.resolve(path.join(__dirname, 'migrations', Repo.SCHEMA_VERSION_ID)),
      options,
      ensureIndexesFn: Repo.ensureIndexes,
      ensureSeedDataFn: Repo.ensureSeedData
    })
  }

  static async ensureIndexes (collection) {
    Repo.ensureIndexesCalled = true
  }

  static async ensureSeedData (collection) {
    Repo.ensureSeedDataCalled = true
  }
}

describe('integration tests of MongoSchemaMigrationRepositorySupport', function () {
  let db
  let repo

  before(async function () {
    this.timeout(10000)

    db = await mongoConnect(process.env.CI_COMMIT_SHA ? { host: 'localhost', port: 27017 } : undefined)
  })

  beforeEach(async function () {
    await dropCollections({ db })

    await MongoSchemaVersionRepository.ensureSchema({ db })

    const collection = await Repo.ensureSchema({ db })
    expect(Repo.ensureIndexesCalled).to.be.true()
    Repo.ensureIndexesCalled = false

    expect(Repo.ensureSeedDataCalled).to.be.true()
    Repo.ensureSeedDataCalled = false

    repo = new Repo({ db, collection })
    const doc = { _id: uuid(), _a: 1 }
    await repo._insert(doc)
  })

  it('should create a version when none exists', async function () {
    const collection = await MongoSchemaVersionRepository.ensureSchema({ db })
    const schemaVersionRepository = new MongoSchemaVersionRepository(collection)

    // the schema version will be set in the beforeEach method

    const schemaVersion = await schemaVersionRepository.findById({ id: Repo.SCHEMA_VERSION_ID })
    expect(semver.eq(schemaVersion.semver, pkg.version)).eq(true)
  })

  it('should ensure seed data and indexes for existing collection with no version', async function () {
    await dropCollections({ db })

    await db.createCollection('test_collection')

    let collection = await MongoSchemaVersionRepository.ensureSchema({ db })
    const schemaVersionRepository = new MongoSchemaVersionRepository(collection)

    collection = await Repo.ensureSchema({ db })
    expect(Repo.ensureIndexesCalled).to.be.true()
    Repo.ensureIndexesCalled = false

    expect(Repo.ensureSeedDataCalled).to.be.true()
    Repo.ensureSeedDataCalled = false

    const schemaVersion = await schemaVersionRepository.findById({ id: Repo.SCHEMA_VERSION_ID })
    expect(semver.eq(schemaVersion.semver, pkg.version)).eq(true)
  })

  it('should perform a schema migration', async function () {
    const baseName = path.resolve(path.join(__dirname, '../repositories/migrations/TestRepo/0.1.0-pre.1'))
    mockfs({
      [baseName]: { /* empty dir */ }
    })

    const migrateFunction = async ({ db, name, schemaVersionRepository, schemaVersion }) => {
      await db.collection(name).updateMany(
        { _testProp: { $exists: false } },
        { $set: { _testProp: 'test' } }
      )
      schemaVersion.semver = '1.0.0-pre.1'
      await schemaVersionRepository.upsert(schemaVersion)
      return db.collection(name)
    }

    mockRequire(baseName, migrateFunction)

    try {
      let collection = await MongoSchemaVersionRepository.ensureSchema({ db })
      const schemaVersionRepository = new MongoSchemaVersionRepository(collection)

      // insert a schema version to force a migration
      await schemaVersionRepository.upsert(new SchemaVersion({
        id: Repo.SCHEMA_VERSION_ID,
        semver: '0.0.1'
      }))

      collection = await Repo.ensureSchema({ db, schemaVersionRepository })
      const documentCount = await collection.countDocuments()
      expect(documentCount).to.be.gte(0)

      const document = await collection.findOne()
      expect(document._testProp).eq('test')

      const schemaVersion = await schemaVersionRepository.findById({ id: Repo.SCHEMA_VERSION_ID })
      expect(semver.eq(schemaVersion.semver, '1.0.0-pre.1')).eq(true)
    } finally {
      mockfs.restore()
      mockRequire.stopAll()
    }
  })

  it('should start up with a schema version less than the current pkg version but no migrations defined', async function () {
    const baseName = path.resolve(path.join(__dirname, '../repositories/migrations/PlatformProvider/'))
    mockfs({
      [baseName]: { /* empty dir */ }
    })

    try {
      let collection = await MongoSchemaVersionRepository.ensureSchema({ db })
      const schemaVersionRepository = new MongoSchemaVersionRepository(collection)

      // insert a schema version to force a migration
      await schemaVersionRepository.upsert(new SchemaVersion({
        id: Repo.SCHEMA_VERSION_ID,
        semver: '0.0.1'
      }))

      collection = await Repo.ensureSchema({ db, schemaVersionRepository })
      const documentCount = await collection.countDocuments()
      expect(documentCount).to.be.gte(0)

      const schemaVersion = await schemaVersionRepository.findById({ id: Repo.SCHEMA_VERSION_ID })
      expect(semver.eq(schemaVersion.semver, '0.0.1')).eq(true)
    } finally {
      mockfs.restore()
      mockRequire.stopAll()
    }
  })

  it('should wait on a locked migration', async function () {
    this.timeout(10000)
    const baseName = path.resolve(path.join(__dirname, '../repositories/migrations/PlatformProvider/1.0.0-pre.1'))
    mockfs({
      [baseName]: { /* empty dir */ }
    })

    try {
      let collection = await MongoSchemaVersionRepository.ensureSchema({ db })
      const schemaVersionRepository = new MongoSchemaVersionRepository(collection)

      // insert an initial locked version
      const schemaVersion = new SchemaVersion({
        id: Repo.SCHEMA_VERSION_ID,
        semver: '0.0.1'
      }).withLocked(MongoSchemaVersionRepository.formatLock(pkg))
      await schemaVersionRepository.upsert(schemaVersion)

      // wait for a time period & unlock the schema
      Promise.delay(3000).then(async () => {
        schemaVersion.locked = false
        await schemaVersionRepository.upsert(schemaVersion)
      })

      // start up the migration & make sure it finishes
      collection = await Repo.ensureSchema({ db, schemaVersionRepository })
      const documentCount = await collection.countDocuments()
      expect(documentCount).to.be.gte(0)
    } finally {
      mockfs.restore()
      mockRequire.stopAll()
    }
  })

  it('should start up correctly with the current package version', async () => {
    let collection = await MongoSchemaVersionRepository.ensureSchema({ db })
    const schemaVersionRepository = new MongoSchemaVersionRepository(collection)

    collection = await Repo.ensureSchema({ db, schemaVersionRepository })
    const documentCount = await collection.countDocuments()
    expect(documentCount).to.be.gte(0)

    const schemaVersion = await schemaVersionRepository.findById({ id: Repo.SCHEMA_VERSION_ID })
    expect(semver.eq(schemaVersion.semver, pkg.version)).eq(true)
  })
})
