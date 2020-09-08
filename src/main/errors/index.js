'use strict'

const { CodedError } = require('@northscaler/error-support')
const MongoRepositoryError = CodedError({ name: 'MongoRepositoryError' })
const MongooseRepositoryError = MongoRepositoryError.subclass({ name: 'MongooseRepositoryError' })

module.exports = {
  MongoRepositoryError,
  MongooseRepositoryError,
  NonuniqueCriteriaError: MongoRepositoryError.subclass({ name: 'NonuniqueCriteriaError' }),
  ObjectNotFoundError: MongoRepositoryError.subclass({ name: 'ObjectNotFoundError' }),
  ObjectExistsError: MongoRepositoryError.subclass({ name: 'ObjectExistsError' }),
  OptimisticLockViolationError: MongoRepositoryError.subclass({ name: 'OptimisticLockViolationError' }),
  UniqueKeyViolationError: MongoRepositoryError.subclass({ name: 'UniqueKeyViolationError' })
}
