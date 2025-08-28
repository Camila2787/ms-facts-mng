"use strict";

const uuidv4 = require("uuid/v4");
const https = require("https");
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

/** GET JSON helper (nativo) */
function fetchJson$(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, res => {
        let data = '';
        res.on('data', chunk => (data += chunk));
        res.on('end', () => {
          try {
            resolve(JSON.parse(data));
          } catch (e) {
            reject(e);
          }
        });
      })
      .on('error', reject);
  });
}

/** Mapea record externo → documento de nuestra colección */
function mapRecordToEntity(rec) {
  rec = rec || {};
  return {
    _id: String(rec.original_order), // requisito del entregable
    date: rec.date,
    year: rec.year,
    type: rec.type,
    country: rec.country,
    area: rec.area,
    location: rec.location,
    activity: rec.activity,
    name: rec.name,
    sex: rec.sex,
    age: rec.age,
    injury: rec.injury,
    fatal_y_n: rec.fatal_y_n,
    time: rec.time,
    species: rec.species,
    investigator_or_source: rec.investigator_or_source,
    pdf: rec.pdf,
    href_formula: rec.href_formula,
    href: rec.href,
    case_number: rec.case_number,
    case_number0: rec.case_number0
  };
}

class SharkAttackCRUD {
  constructor() {}

  /**     
   * Generates and returns an object that defines the CQRS request handlers.
   * 
   * The map is a relationship of: AGGREGATE_TYPE VS { MESSAGE_TYPE VS  { fn: rxjsFunction, instance: invoker_instance } }
   */
  generateRequestProcessorMap() {
    return {
      'SharkAttack': {
        "emigateway.graphql.query.FactsMngSharkAttackListing": { fn: instance.getFactsMngSharkAttackListing$, instance, jwtValidation: { roles: READ_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "emigateway.graphql.query.FactsMngSharkAttack": { fn: instance.getSharkAttack$, instance, jwtValidation: { roles: READ_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "emigateway.graphql.mutation.FactsMngCreateSharkAttack": { fn: instance.createSharkAttack$, instance, jwtValidation: { roles: WRITE_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "emigateway.graphql.mutation.FactsMngUpdateSharkAttack": { fn: instance.updateSharkAttack$, jwtValidation: { roles: WRITE_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "emigateway.graphql.mutation.FactsMngDeleteSharkAttacks": { fn: instance.deleteSharkAttacks$, jwtValidation: { roles: WRITE_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        // >>> IMPORTACIÓN MASIVA (PARTE 2)
        "emigateway.graphql.mutation.FactsMngImportSharkAttacks": { fn: instance.importSharkAttacks$, instance, jwtValidation: { roles: WRITE_ROLES, attributes: REQUIRED_ATTRIBUTES } },
      }
    }
  };

  /**  
   * Gets the SharkAttack list
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
    const originalOrder = args && args.input ? args.input.original_order : undefined;
    const aggregateId = (originalOrder !== undefined && originalOrder !== null)
      ? String(originalOrder)
      : uuidv4();

    const input = { ...args.input };
    if (typeof input.active === 'undefined') {
      input.active = true;
    }
    if (!input._id) {
      input._id = aggregateId;
    }

    return SharkAttackDA.createSharkAttack$(aggregateId, input, authToken.preferred_username).pipe(
      mergeMap(aggregate => forkJoin(
        CqrsResponseHelper.buildSuccessResponse$(aggregate),
        eventSourcing.emitEvent$(
          instance.buildAggregateMofifiedEvent('CREATE', 'SharkAttack', aggregateId, authToken, aggregate),
          { autoAcknowledgeKey: process.env.MICROBACKEND_KEY }
        ),
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
   * IMPORTACIÓN MASIVA (Parte 2)
   * - Consume OpenDataSoft (100 registros)
   * - Usa original_order como _id
   * - Inserta/upserta
   * - Emite evento ES (AggregateType=SharkAttact, EventType=Reported)
   * - Devuelve [ids]
   */
  importSharkAttacks$() {
    const URL = 'https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/global-shark-attack/records?limit=100';

    return from(fetchJson$(URL)).pipe(
      map(function (body) {
        return (body && Array.isArray(body.results)) ? body.results : [];
      }),
      mergeMap(function (results) { return from(results); }),
      map(mapRecordToEntity),
      mergeMap(function (doc) {
        // Inserta si no existe
        return SharkAttackDA.createIfNotExists$(doc).pipe(
          // Evento de dominio (Event Sourcing)
          mergeMap(function () {
            const ev = new Event({
              eventType: 'Reported',
              eventTypeVersion: 1,
              aggregateType: 'SharkAttact',   // OJO: exactamente así lo pide el criterio
              aggregateId: doc._id,
              data: doc,
              user: 'system-import'
            });
            return eventSourcing.emitEvent$(ev, { autoAcknowledgeKey: process.env.MICROBACKEND_KEY });
          }),
          map(function () { return doc._id; }),
          catchError(function (err) {
            // Si es duplicado, devolvemos el id igual
            if (err && (err.code === 11000 || err.name === 'MongoError')) {
              return of(doc._id);
            }
            throw err;
          })
        );
      }),
      toArray(),
      mergeMap(function (ids) { return CqrsResponseHelper.buildSuccessResponse$(ids); }),
      catchError(function (err) {
        return iif(function () { return err.name === 'MongoTimeoutError'; }, throwError(err), CqrsResponseHelper.handleError$(err));
      })
    );
  }

  /**
   * Generate an Modified event 
   * @param {string} modType 'CREATE' | 'UPDATE' | 'DELETE'
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
