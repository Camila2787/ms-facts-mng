'use strict';

const axios = require('axios');
const { from } = require('rxjs');
const { map, mergeMap, toArray } = require('rxjs/operators');
const SharkAttackDA = require('../domain/shark-attack/data-access/SharkAttackDA');
const { SharkAttackReported } = require('../bin/SharkAttackES');

// URL pública para obtener los datos
const URL = 'https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/global-shark-attack/records?limit=100';

// Mapea el registro recibido de la API a la estructura esperada en Mongo
function mapRecordToEntity(record) {
  const f = record || {};
  return {
    _id: String(f.original_order), // IDunico
    year: f.year,
    type: f.type,
    country: f.country,
    area: f.area,
    location: f.location,
    activity: f.activity,
    name: f.name,
    sex: f.sex,
    age: f.age,
    injury: f.injury,
    fatal_y_n: f.fatal_y_n,
    time: f.time,
    species: f.species,
    investigator_or_source: f.investigator_or_source,
    pdf: f.pdf,
    href_formula: f.href_formula,
    href: f.href,
    case_number: f.case_number,
    case_number0: f.case_number0,
  };
}

/**
 * Servicio para importar ataques de tiburones.
 * - Consume la API externa.
 * - Persiste los registros en MongoDB.
 * - Genera un evento de dominio por cada registro.
 */
exports.importSharkAttacksService = () => {
  return from(axios.get(URL)).pipe(
    map(res => res.data?.results || res.data?.records || []), // depende de la respuesta de la API
    mergeMap(arr => from(arr)),
    map(mapRecordToEntity),
    mergeMap(doc =>
      from(SharkAttackDA.saveSharkAttack$(doc)).pipe( // Guardar documento
        mergeMap(() => from(SharkAttackReported(doc))) // Emitir evento Reported
      )
    ),
    toArray() // Devolvemos los 100 registros insertados
  );
};
