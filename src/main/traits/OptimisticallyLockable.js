const { Trait } = require('@northscaler/mutrait')
const property = require('@northscaler/property-decorator')
const { OptimisticLockViolationError } = require('../errors')

/**
 * Imparts an `optimisticLock` read-only property with backing property `_optimisticLock`, and supporting methods.
 */
const OptimisticallyLockable = Trait(superclass =>
  class extends superclass {
    @property({ set: false })
    _optimisticLock

    /**
     * Determines if the given object's optimistic lock matches `this._optimisticLock`.
     *
     * @param {object} that The other object whose lock will be compared.
     * @return {boolean}
     * @private
     */
    _optimisticLockMatches (that) {
      return this._optimisticLock === that?._optimisticLock
    }

    /**
     * Verifies that the optimistic lock of the given object matches `this._optimisticLock`, else throws an `OptimisticLockViolationError`.
     *
     * @param that
     * @private
     */
    _verifyOptimisticLock (that) {
      if (this._optimisticLockMatches(that)) return

      throw new OptimisticLockViolationError({
        message: 'optimistic lock violation',
        info: { thiz: this, that }
      })
    }
  }
)

module.exports = OptimisticallyLockable
