"use strict";

const { empty, Observable, of, from } = require("rxjs");
const { map, mergeMap, toArray, catchError } = require('rxjs/operators');

const SharkAttackCRUD = require("./SharkAttackCRUD")();
const SharkAttackES = require("./SharkAttackES")();
const DataAcess = require("./data-access/");
const https = require('https');
const SharkAttackDA = require('./data-access/SharkAttackDA');

// -------- GET simple usando https nativo (sin instalar librerías) --------
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

const IMPORT_URL =
  'https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/global-shark-attack/records?limit=100';

function mapRecordToEntity(rec = {}) {
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
    case_number0: rec.case_number0,
  };
}

// -------- Persistencia defensiva: usa el método que exista en tu DA --------
function persistSharkAttack$(doc) {
  if (typeof SharkAttackDA.saveSharkAttack$ === 'function') {
    return SharkAttackDA.saveSharkAttack$(doc);
  }
  if (typeof SharkAttackDA.upsertSharkAttack$ === 'function') {
    return SharkAttackDA.upsertSharkAttack$(doc);
  }
  if (typeof SharkAttackDA.createSharkAttack$ === 'function') {
    return SharkAttackDA.createSharkAttack$(doc);
  }
  if (typeof SharkAttackDA.updateSharkAttackFromRecovery$ === 'function') {
    // signatura usada por ES en tu proyecto: (aid, aggregateData, av)
    return SharkAttackDA.updateSharkAttackFromRecovery$(doc._id, doc, 1);
  }
  throw new Error('No persistence method found in SharkAttackDA');
}

// -------- Handler CQRS que atiende la mutación del gateway --------
function FactsMngImportSharkAttacks$() {
  return from(fetchJson$(IMPORT_URL)).pipe(
    map(res => res?.results || res?.records || []),
    mergeMap(arr => from(arr)),
    map(mapRecordToEntity),
    mergeMap(doc => persistSharkAttack$(doc).pipe(map(() => doc._id))),
    toArray(),
    map(ids => ({ result: { code: 200 }, data: ids })),
    catchError(err => of({ result: { code: 500, error: err?.message || String(err) }, data: null }))
  );
}

// -------- Mapa base del CRUD para extender con nuestra mutación --------
const baseCqrsMap = SharkAttackCRUD.generateRequestProcessorMap();

module.exports = {
  /**
   * domain start workflow
   */
  start$: DataAcess.start$,
  /**
   * start for syncing workflow
   * @returns {Observable}
   */
  startForSyncing$: DataAcess.start$,
  /**
   * start for getting ready workflow
   * @returns {Observable}
   */
  startForGettingReady$: empty(),
  /**
   * Stop workflow
   * @returns {Observable}
   */
  stop$: DataAcess.stop$,
  /**
   * @returns {SharkAttackCRUD}
   */
  SharkAttackCRUD: SharkAttackCRUD,
  /**
   * CRUD request processors Map + (EXT) handler de importación
   */
  cqrsRequestProcessorMap: {
    ...baseCqrsMap,
    // 👇 Tópico EXACTO que publica el emi-gateway (mutación)
    'emigateway.graphql.mutation.FactsMngImportSharkAttacks': {
      fn: FactsMngImportSharkAttacks$,
      instance: null
    }
  },
  /**
   * @returns {SharkAttackES}
   */
  SharkAttackES,
  /**
   * EventSoircing event processors Map
   */
  eventSourcingProcessorMap: SharkAttackES.generateEventProcessorMap(),
};
