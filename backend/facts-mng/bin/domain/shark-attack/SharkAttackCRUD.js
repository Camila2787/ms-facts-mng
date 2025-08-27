"use strict";

const uuidv4 = require("uuid/v4");
const { of, forkJoin, from, iif, throwError } = require("rxjs");
const { mergeMap, catchError, map, toArray, pluck } = require('rxjs/operators');

const Event = require("@nebulae/event-store").Event;
const { CqrsResponseHelper } = require('@nebulae/backend-node-tools').cqrs;
const { ConsoleLogger } = require('@nebulae/backend-node-tools').log;
const { CustomError, INTERNAL_SERVER_ERROR_CODE, PERMISSION_DENIED } = require("@nebulae/backend-node-tools").error;
const { brokerFactory } = require("@nebulae/backend-node-tools").broker;

const broker = brokerFactory();
const eventSourcing = require("../../tools/event-sourcing").eventSourcing;
const SharkAttackDA = require("./data-access/SharkAttackDA");

const READ_ROLES = ["SHARK_ATTACK_READ"];
const WRITE_ROLES = ["SHARK_ATTACK_WRITE"];
const REQUIRED_ATTRIBUTES = [];
const MATERIALIZED_VIEW_TOPIC = "emi-gateway-materialized-view-updates";

/**
 * Singleton instance
 * @type { SharkAttackCRUD }
 */
let instance;

class SharkAttackCRUD {
  constructor() {
  }

  /**     
   * Generates and returns an object that defines the CQRS request handlers.
   * 
   * The map is a relationship of: AGGREGATE_TYPE VS { MESSAGE_TYPE VS  { fn: rxjsFunction, instance: invoker_instance } }
   * 
   * ## Example
   *  { "CreateUser" : { "somegateway.someprotocol.mutation.CreateUser" : {fn: createUser$, instance: classInstance } } }
   */
  generateRequestProcessorMap() {
    return {
      'SharkAttack': {
        "emigateway.graphql.query.FactsMngSharkAttackListing": { fn: instance.getFactsMngSharkAttackListing$, instance, jwtValidation: { roles: READ_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "emigateway.graphql.query.FactsMngSharkAttack": { fn: instance.getSharkAttack$, instance, jwtValidation: { roles: READ_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "emigateway.graphql.mutation.FactsMngCreateSharkAttack": { fn: instance.createSharkAttack$, instance, jwtValidation: { roles: WRITE_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "emigateway.graphql.mutation.FactsMngUpdateSharkAttack": { fn: instance.updateSharkAttack$, jwtValidation: { roles: WRITE_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "emigateway.graphql.mutation.FactsMngDeleteSharkAttacks": { fn: instance.deleteSharkAttacks$, jwtValidation: { roles: WRITE_ROLES, attributes: REQUIRED_ATTRIBUTES } },
      }
    }
  };


  /**  
   * Gets the SharkAttack list
   *
   * @param {*} args args
   */
  getFactsMngSharkAttackListing$({ args }, authToken) {
    const { filterInput, paginationInput, sortInput } = args;
    const { queryTotalResultCount = false } = paginationInput || {};

    return forkJoin(
      SharkAttackDA.getSharkAttackList$(filterInput, paginationInput, sortInput).pipe(toArray()),
      queryTotalResultCount ? SharkAttackDA.getSharkAttackSize$(filterInput) : of(undefined),
    ).pipe(
      map(([listing, queryTotalResultCount]) => ({ listing, queryTotalResultCount })),
      mergeMap(rawResponse => CqrsResponseHelper.buildSuccessResponse$(rawResponse)),
      catchError(err => iif(() => err.name === 'MongoTimeoutError', throwError(err), CqrsResponseHelper.handleError$(err)))
    );
  }

  /**  
   * Gets the get SharkAttack by id
   *
   * @param {*} args args
   */
  getSharkAttack$({ args }, authToken) {
    const { id, organizationId } = args;
    return SharkAttackDA.getSharkAttack$(id, organizationId).pipe(
      mergeMap(rawResponse => CqrsResponseHelper.buildSuccessResponse$(rawResponse)),
      catchError(err => iif(() => err.name === 'MongoTimeoutError', throwError(err), CqrsResponseHelper.handleError$(err)))
    );

  }


  /**
  * Create a SharkAttack
  */
  createSharkAttack$({ root, args, jwt }, authToken) {
    const aggregateId = uuidv4();
    const input = {
      active: false,
      ...args.input,
    };

    return SharkAttackDA.createSharkAttack$(aggregateId, input, authToken.preferred_username).pipe(
      mergeMap(aggregate => forkJoin(
        CqrsResponseHelper.buildSuccessResponse$(aggregate),
        eventSourcing.emitEvent$(instance.buildAggregateMofifiedEvent('CREATE', 'SharkAttack', aggregateId, authToken, aggregate), { autoAcknowledgeKey: process.env.MICROBACKEND_KEY }),
        broker.send$(MATERIALIZED_VIEW_TOPIC, `FactsMngSharkAttackModified`, aggregate)
      )),
      map(([sucessResponse]) => sucessResponse),
      catchError(err => iif(() => err.name === 'MongoTimeoutError', throwError(err), CqrsResponseHelper.handleError$(err)))
    )
  }

  /**
   * updates an SharkAttack 
   */
  updateSharkAttack$({ root, args, jwt }, authToken) {
    const { id, input, merge } = args;

    return (merge ? SharkAttackDA.updateSharkAttack$ : SharkAttackDA.replaceSharkAttack$)(id, input, authToken.preferred_username).pipe(
      mergeMap(aggregate => forkJoin(
        CqrsResponseHelper.buildSuccessResponse$(aggregate),
        eventSourcing.emitEvent$(instance.buildAggregateMofifiedEvent(merge ? 'UPDATE_MERGE' : 'UPDATE_REPLACE', 'SharkAttack', id, authToken, aggregate), { autoAcknowledgeKey: process.env.MICROBACKEND_KEY }),
        broker.send$(MATERIALIZED_VIEW_TOPIC, `FactsMngSharkAttackModified`, aggregate)
      )),
      map(([sucessResponse]) => sucessResponse),
      catchError(err => iif(() => err.name === 'MongoTimeoutError', throwError(err), CqrsResponseHelper.handleError$(err)))
    )
  }


  /**
   * deletes an SharkAttack
   */
  deleteSharkAttacks$({ root, args, jwt }, authToken) {
    const { ids } = args;
    return forkJoin(
      SharkAttackDA.deleteSharkAttacks$(ids),
      from(ids).pipe(
        mergeMap(id => eventSourcing.emitEvent$(instance.buildAggregateMofifiedEvent('DELETE', 'SharkAttack', id, authToken, {}), { autoAcknowledgeKey: process.env.MICROBACKEND_KEY })),
        toArray()
      )
    ).pipe(
      map(([ok, esResps]) => ({ code: ok ? 200 : 400, message: `SharkAttack with id:s ${JSON.stringify(ids)} ${ok ? "has been deleted" : "not found for deletion"}` })),
      mergeMap((r) => forkJoin(
        CqrsResponseHelper.buildSuccessResponse$(r),
        broker.send$(MATERIALIZED_VIEW_TOPIC, `FactsMngSharkAttackModified`, { id: 'deleted', name: '', active: false, description: '' })
      )),
      map(([cqrsResponse, brokerRes]) => cqrsResponse),
      catchError(err => iif(() => err.name === 'MongoTimeoutError', throwError(err), CqrsResponseHelper.handleError$(err)))
    );
  }


  /**
   * Generate an Modified event 
   * @param {string} modType 'CREATE' | 'UPDATE' | 'DELETE'
   * @param {*} aggregateType 
   * @param {*} aggregateId 
   * @param {*} authToken 
   * @param {*} data 
   * @returns {Event}
   */
  buildAggregateMofifiedEvent(modType, aggregateType, aggregateId, authToken, data) {
    return new Event({
      eventType: `${aggregateType}Modified`,
      eventTypeVersion: 1,
      aggregateType: aggregateType,
      aggregateId,
      data: {
        modType,
        ...data
      },
      user: authToken.preferred_username
    })
  }
}

/**
 * @returns {SharkAttackCRUD}
 */
module.exports = () => {
  if (!instance) {
    instance = new SharkAttackCRUD();
    ConsoleLogger.i(`${instance.constructor.name} Singleton created`);
  }
  return instance;
};
