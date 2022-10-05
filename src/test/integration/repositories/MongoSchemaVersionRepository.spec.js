/* global describe, it, before, beforeEach */
'use strict'

const chai = require('chai')
chai.use(require('chai-as-promised'))
chai.use(require('dirty-chai'))
const expect = chai.expect

const uuid = require('uuid').v4
const { ObjectNotFoundError } = require('../../../main/errors')
const { mongoConnect, dropCollections } = require('@ballistagroup/mongo-test-support')
const { SchemaVersion } = require('../../../main/entities')
const { MongoSchemaVersionRepository } = require('../../../main/repositories')

describe('integration tests of MongoSchemaVersionRepository', function () {
  let db
  let repo

  before(async function () {
    this.timeout(10000)
    db = await mongoConnect(process.env.CI_COMMIT_SHA ? { host: 'localhost', port: 27017 } : undefined)
  })

  beforeEach(async function () {
    await dropCollections({ db, names: ['schema_versions'] })

    const collection = await MongoSchemaVersionRepository.ensureSchema({ db })
    repo = new MongoSchemaVersionRepository(collection)
  })

  it('should store & retrieve via finder methods', async function () {
    const locker = MongoSchemaVersionRepository.DEFAULT_LOCK
    const sv = new SchemaVersion({ id: 'foobar', semver: '0.1.0-pre.0' }).withLocked(locker)
    await repo.upsert(sv)
    let fromDb = await repo.getById({ id: sv.id })
    expect(sv).to.deep.equal(fromDb)
    try {
      await repo.getById(uuid())
    } catch (e) {
      expect(e).to.be.instanceOf(ObjectNotFoundError)
    }

    sv.locked = false
    await repo.upsert(sv)
    fromDb = await repo.getById({ id: sv.id })
    expect(sv).to.deep.equal(fromDb)
    try {
      await repo.getById(uuid())
    } catch (e) {
      expect(e).to.be.instanceOf(ObjectNotFoundError)
    }
  })
})
