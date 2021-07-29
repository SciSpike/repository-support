'use strict'

const fs = require('fs')
const MongoSchemaVersionRepository = require('../repositories/MongoSchemaVersionRepository')
const SchemaVersion = require('../entities/SchemaVersion')
const { Trait } = require('@northscaler/mutrait')
const { MissingRequiredArgumentError } = require('@northscaler/error-support')
const semver = require('semver')
const Promise = require('bluebird')

const MongoSchemaMigrationRepositorySupport = Trait(superclass =>
  class extends superclass {
    /**
     * Create and / or migrate schema
     * @protected
     * @param db
     * @param name
     * @param schemaVersionId
     * @param pkg package.json
     * @param dirName __dirName from subclass
     * @param options
     * @returns {Promise<Collection>}
     */
    static async _ensureSchema ({
      db,
      name,
      schemaVersionId,
      pkg,
      migrationsDir,
      options,
      ensureIndexesFn,
      ensureSeedDataFn
    }) {
      if (!db) throw MissingRequiredArgumentError({ message: 'db required' })
      if (!name) throw MissingRequiredArgumentError({ message: 'name required' })
      if (!schemaVersionId) throw MissingRequiredArgumentError({ message: 'schema document id required' })

      const collection = await MongoSchemaVersionRepository.ensureSchema({ db })
      const schemaVersionRepository = new MongoSchemaVersionRepository(collection)

      let schemaVersion = await schemaVersionRepository.findById({ id: schemaVersionId })

      if (!schemaVersion) { // never done before or schemaVersion missing
        schemaVersion = new SchemaVersion({
          id: schemaVersionId,
          semver: pkg.version
        }).withLocked(MongoSchemaVersionRepository.formatLock(pkg))

        await schemaVersionRepository.upsert(schemaVersion)

        if ((await db.collections()).map(it => it.collectionName).includes(name)) {
          const collection = db.collection(name)
          if (ensureIndexesFn) await ensureIndexesFn(collection)
          if (ensureSeedDataFn) await ensureSeedDataFn(collection)
          await schemaVersionRepository.upsert(schemaVersion.withLocked(false))
          return collection
        }

        const collection = await db.createCollection(name, options)

        if (ensureIndexesFn) await ensureIndexesFn(collection)
        if (ensureSeedDataFn) await ensureSeedDataFn(collection)

        await schemaVersionRepository.upsert(schemaVersion.withLocked(false))
        return collection
      }

      if (schemaVersion.gte(pkg.version)) { // we're current. no need to migrate
        return db.collection(name)
      }

      if (!schemaVersion.locked) {
        // read ./migrations/<SCHEMA_VERSION_ID>
        // for dir names > schemaVersion.semver & <= pkg.version,
        // then execute their exported function passing in db, name & capturing returned collection
        let migrations = []
        if (fs.existsSync(migrationsDir)) {
          migrations = fs.readdirSync(migrationsDir, { withFileTypes: true })
            .filter(entry =>
              entry.isDirectory() &&
              semver.valid(entry.name) &&
              schemaVersion.lt(entry.name) &&
              semver.lt(entry.name, pkg.version))
            .map(entry => entry.name)
            .sort((a, b) => semver.compare(a, b))
        }

        if (migrations.length) {
          schemaVersion.locked = MongoSchemaVersionRepository.formatLock(pkg)
          await schemaVersionRepository.upsert(schemaVersion)
        }

        let collection
        for (const migration of migrations) {
          const migrate = require(`${migrationsDir}/${migration}`) // must export: async function ({db, name, schemaVersionRepository, schemaVersion})
          collection = await migrate({ db, name, schemaVersionRepository, schemaVersion })
        }

        // if there are no migration scripts defined or no migrations directory for the collection, the
        // collection will be empty, so just return the current specified collection
        if (!collection) {
          collection = db.collection(name)
        }

        if (ensureIndexesFn) await ensureIndexesFn(collection)
        if (ensureSeedDataFn) await ensureSeedDataFn(collection)

        await schemaVersionRepository.upsert(schemaVersion.withLocked(false))

        return collection
      } else {
        // if this schema version is locked, another process is migrating
        // wait until the migration is done then return the correct collection
        let collection

        while (schemaVersion.locked) {
          await Promise.delay((options?.schemaVersionLockPauseSeconds || 1) * 1000)
          schemaVersion = await schemaVersionRepository.findById({ id: schemaVersionId })

          if (!schemaVersion.locked) {
            collection = db.collection(name)
            break
          }
        }

        return collection
      }
    }
  }
)

module.exports = MongoSchemaMigrationRepositorySupport
