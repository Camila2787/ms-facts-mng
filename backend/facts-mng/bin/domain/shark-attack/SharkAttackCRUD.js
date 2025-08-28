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

// ---- Helpers para importación ----
const IMPORT_URL =
  'https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/global-shark-attack/records?limit=100';

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, res => {
      let data = '';
      res.on('data', chunk => (data += chunk));
      res.on('end', () => {
        try { resolve(JSON.parse(data)); } catch (e) { reject(e); }
      });
    }).on('error', reject);
  });
}

function recordToEntity(r, organizationId) {
  // Soporta tanto {results:[...]} como {records:[{record:{fields}}]}
  const rec = (r && r.record && r.record.fields) ? r.record.fields : r || {};
  return {
    _id: String(rec.original_order),
    organizationId,
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
    case_number0: rec.case_number0,
    active: true,
  };
}

/**
 * Singleton instance
 * @type { SharkAttackCRUD }
 */
let instance;

class SharkAttackCRUD {
  constructor() {}

  /**     
   * Generates and returns an object that defines the CQRS request handlers.
   */
  generateRequestProcessorMap() {
    return {
      'SharkAttack': {
        "emigateway.graphql.query.FactsMngSharkAttackListing": { fn: instance.getFactsMngSharkAttackListing$, instance, jwtValidation: { roles: READ_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "emigateway.graphql.query.FactsMngSharkAttack": { fn: instance.getSharkAttack$, instance, jwtValidation: { roles: READ_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "emigateway.graphql.mutation.FactsMngCreateSharkAttack": { fn: instance.createSharkAttack$, instance, jwtValidation: { roles: WRITE_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "emigateway.graphql.mutation.FactsMngUpdateSharkAttack": { fn: instance.updateSharkAttack$, jwtValidation: { roles: WRITE_ROLES, attributes: REQUIRED_ATTRIBUTES } },
        "emigateway.graphql.mutation.FactsMngDeleteSharkAttacks": { fn: instance.deleteSharkAttacks$, jwtValidation: { roles: WRITE_ROLES, attributes: REQUIRED_ATTRIBUTES } },

        // 🚀 Mutación de importación (PARTE 2)
        "emigateway.graphql.mutation.FactsMngImportSharkAttacks": { fn: instance.importSharkAttacks$, instance, jwtValidation: { roles: WRITE_ROLES, attributes: REQUIRED_ATTRIBUTES } },
      }
    }
  };

  // ---------- LISTING ----------
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

  // ---------- GET ----------
  getSharkAttack$({ args }, authToken) {
    const { id, organizationId } = args;
    return SharkAttackDA.getSharkAttack$(id, organizationId).pipe(
      mergeMap(rawResponse => CqrsResponseHelper.buildSuccessResponse$(rawResponse)),
      catchError(err => iif(() => err.name === 'MongoTimeoutError', throwError(err), CqrsResponseHelper.handleError$(err)))
    );
  }

  // ---------- CREATE ----------
  createSharkAttack$({ root, args, jwt }, authToken) {
    const originalOrder = args?.input?.original_order;
    const aggregateId = (originalOrder !== undefined && originalOrder !== null)
      ? String(originalOrder)
      : uuidv4();

    const input = { ...args.input };
    if (typeof input.active === 'undefined') { input.active = true; }
    if (!input._id) { input._id = aggregateId; }

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

  // ---------- UPDATE ----------
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

  // ---------- DELETE ----------
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

  // ---------- IMPORT (PARTE 2) ----------
  importSharkAttacks$({ root, args, jwt }, authToken) {
    // ⚠️ IMPORTANTE: pon aquí el mismo organizationId que usa tu usuario en el front
    // Puedes setearlo por env: export DEFAULT_ORG_ID=<tu_org_id>
    const organizationId =
      process.env.DEFAULT_ORG_ID ||
      // si tu JWT trae un claim con la org, ajústalo aquí:
      authToken?.organizationId ||
      "ORG-DEFAULT";

    return from(fetchJson(IMPORT_URL)).pipe(
      mergeMap(raw => from((raw && (raw.results || raw.records)) || [])),
      map(rec => recordToEntity(rec, organizationId)),
      mergeMap(entity =>
        // Insertar; si ya existe, actualizar
        SharkAttackDA.createSharkAttack$(entity._id, entity, authToken.preferred_username).pipe(
          catchError(err =>
            (err && err.code === 11000)
              ? SharkAttackDA.updateSharkAttack$(entity._id, entity, authToken.preferred_username)
              : throwError(err)
          ),
          mergeMap(saved =>
            forkJoin(
              // Evento de modificación estándar (mantiene MV en sync)
              eventSourcing.emitEvent$(
                instance.buildAggregateMofifiedEvent('CREATE', 'SharkAttack', entity._id, authToken, saved),
                { autoAcknowledgeKey: process.env.MICROBACKEND_KEY }
              ),
              // ✅ Evento SOLICITADO por el entregable
              eventSourcing.emitEvent$(new Event({
                eventType: 'Reported',
                eventTypeVersion: 1,
                aggregateType: 'SharkAttact', // tal cual lo pide el criterio (con esa grafía)
                aggregateId: entity._id,
                data: saved,
                user: authToken.preferred_username
              }), { autoAcknowledgeKey: process.env.MICROBACKEND_KEY }),
              // Notificación para refrescar el front
              broker.send$(MATERIALIZED_VIEW_TOPIC, `FactsMngSharkAttackModified`, saved)
            ).pipe(map(() => saved && (saved._id || saved.id || entity._id)))
          )
        )
      ),
      toArray(), // -> [ids]
      mergeMap(ids => CqrsResponseHelper.buildSuccessResponse$(ids)),
      catchError(err => CqrsResponseHelper.handleError$(err))
    );
  }

  /**
   * Generate an Modified event 
   */
  buildAggregateMofifiedEvent(modType, aggregateType, aggregateId, authToken, data) {
    return new Event({
      eventType: `${aggregateType}Modified`,
      eventTypeVersion: 1,
      aggregateType: aggregateType,
      aggregateId,
      data: { modType, ...data },
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
