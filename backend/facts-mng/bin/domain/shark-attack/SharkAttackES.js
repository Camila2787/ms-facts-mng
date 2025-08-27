'use strict'

const { iif } = require("rxjs");
const { tap } = require('rxjs/operators');
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;

const SharkAttackDA = require("./data-access/SharkAttackDA");
/**
 * Singleton instance
 * @type { SharkAttackES }
 */
let instance;

class SharkAttackES {

    constructor() {
    }

    /**     
     * Generates and returns an object that defines the Event-Sourcing events handlers.
     * 
     * The map is a relationship of: AGGREGATE_TYPE VS { EVENT_TYPE VS  { fn: rxjsFunction, instance: invoker_instance } }
     * 
     * ## Example
     *  { "User" : { "UserAdded" : {fn: handleUserAdded$, instance: classInstance } } }
     */
    generateEventProcessorMap() {
        return {
            'SharkAttack': {
                "SharkAttackModified": { fn: instance.handleSharkAttackModified$, instance, processOnlyOnSync: true },
            }
        }
    };

    /**
     * Using the SharkAttackModified events restores the MaterializedView
     * This is just a recovery strategy
     * @param {*} SharkAttackModifiedEvent SharkAttack Modified Event
     */
    handleSharkAttackModified$({ etv, aid, av, data, user, timestamp }) {
        const aggregateDataMapper = [
            /*etv=0 mapper*/ () => { throw new Error('etv 0 is not an option') },
            /*etv=1 mapper*/ (eventData) => { return { ...eventData, modType: undefined }; }
        ];
        delete aggregateDataMapper.modType;
        const aggregateData = aggregateDataMapper[etv](data);
        return iif(
            () => (data.modType === 'DELETE'),
            SharkAttackDA.deleteSharkAttack$(aid),
            SharkAttackDA.updateSharkAttackFromRecovery$(aid, aggregateData, av)
        ).pipe(
            tap(() => ConsoleLogger.i(`SharkAttackES.handleSharkAttackModified: ${data.modType}: aid=${aid}, timestamp=${timestamp}`))
        )
    }
}


/**
 * @returns {SharkAttackES}
 */
module.exports = () => {
    if (!instance) {
        instance = new SharkAttackES();
        ConsoleLogger.i(`${instance.constructor.name} Singleton created`);
    }
    return instance;
};