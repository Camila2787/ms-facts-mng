'use strict';

const { of } = require('rxjs');
const { map, catchError } = require('rxjs/operators');
const { importSharkAttacksService$ } = require('../application/importSharkAttacks.service');

/**
 * Registra los handlers de mutaciones GraphQL para el emi-gateway
 * @param {*} broker instancia del broker inicializada
 */
function registerGraphqlMutations(broker) {
  broker
    .onBrokerMessage$(
      'SharkAttack', // aggregateName: SharkAttack
      'emigateway.graphql.mutation.FactsMngImportSharkAttacks', // topic de la mutación
      () =>
        importSharkAttacksService$().pipe(
          map(list => ({
            result: { code: 200 },
            data: list.map(x => x._id), // devuelve los IDs como pide el schema [ID!]!
          })),
          catchError(err =>
            of({
              result: { code: 500, error: err?.message || String(err) },
              data: null,
            })
          )
        )
    )
    .subscribe(
      () => {},
      err => console.error('Error en FactsMngImportSharkAttacks:', err),
      () => console.log('Handler FactsMngImportSharkAttacks detenido')
    );
}

module.exports = { registerGraphqlMutations };
