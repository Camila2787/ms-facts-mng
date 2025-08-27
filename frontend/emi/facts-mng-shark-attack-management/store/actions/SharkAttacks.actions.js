import { defer } from 'rxjs';
import { mergeMap, map } from 'rxjs/operators';

import graphqlService from '../../../../services/graphqlService';
import { FactsMngSharkAttackListing, FactsMngDeleteSharkAttack } from '../../gql/SharkAttack';

export const SET_SHARK_ATTACKS = '[SHARK_ATTACK_MNG] SET SHARK_ATTACKS';
export const SET_SHARK_ATTACKS_PAGE = '[SHARK_ATTACK_MNG] SET SHARK_ATTACKS PAGE';
export const SET_SHARK_ATTACKS_ROWS_PER_PAGE = '[SHARK_ATTACK_MNG] SET SHARK_ATTACKS ROWS PER PAGE';
export const SET_SHARK_ATTACKS_ORDER = '[SHARK_ATTACK_MNG] SET SHARK_ATTACKS ORDER';
export const SET_SHARK_ATTACKS_FILTERS_ORGANIZATION_ID = '[SHARK_ATTACK_MNG] SET SHARK_ATTACKS FILTERS ORGANIZATION_ID';
export const SET_SHARK_ATTACKS_FILTERS_NAME = '[SHARK_ATTACK_MNG] SET SHARK_ATTACKS FILTERS NAME';
export const SET_SHARK_ATTACKS_FILTERS_ACTIVE = '[SHARK_ATTACK_MNG] SET SHARK_ATTACKS FILTERS ACTIVE';

/**
 * Common function to generate the arguments for the FactsMngSharkAttackListing query based on the user input
 * @param {Object} queryParams 
 */
function getListingQueryArguments({ filters: { name, organizationId, active }, order, page, rowsPerPage }) {
    const args = {
        "filterInput": { organizationId },
        "paginationInput": { "page": page, "count": rowsPerPage, "queryTotalResultCount": (page === 0) },
        "sortInput": order.id ? { "field": order.id, "asc": order.direction === "asc" } : undefined
    };
    if (name.trim().length > 0) {
        args.filterInput.name = name;
    }
    if (active !== null) {
        args.filterInput.active = active;
    }
    return args;
}

/**
 * Queries the SharkAttack Listing based on selected filters, page and order
 * @param {{ filters, order, page, rowsPerPage }} queryParams
 */
export function getSharkAttacks({ filters, order, page, rowsPerPage }) {
    const args = getListingQueryArguments({ filters, order, page, rowsPerPage });    
    return (dispatch) => graphqlService.client.query(FactsMngSharkAttackListing(args)).then(result => {
        return dispatch({
            type: SET_SHARK_ATTACKS,
            payload: result.data.FactsMngSharkAttackListing
        });
    })
}

/**
 * Executes the mutation to remove the selected rows
 * @param {*} selectedForRemovalIds 
 * @param {*} param1 
 */
export function removeSharkAttacks(selectedForRemovalIds, { filters, order, page, rowsPerPage }) {
    const deleteArgs = { ids: selectedForRemovalIds };
    const listingArgs = getListingQueryArguments({ filters, order, page, rowsPerPage });
    return (dispatch) => defer(() => graphqlService.client.mutate(FactsMngDeleteSharkAttack(deleteArgs))).pipe(
        mergeMap(() => defer(() => graphqlService.client.query(FactsMngSharkAttackListing(listingArgs)))),
        map((result) =>
            dispatch({
                type: SET_SHARK_ATTACKS,
                payload: result.data.FactsMngSharkAttackListing
            })
        )
    ).toPromise();
}

/**
 * Set the listing page
 * @param {int} page 
 */
export function setSharkAttacksPage(page) {
    return {
        type: SET_SHARK_ATTACKS_PAGE,
        page
    }
}

/**
 * Set the number of rows to see per page
 * @param {*} rowsPerPage 
 */
export function setSharkAttacksRowsPerPage(rowsPerPage) {
    return {
        type: SET_SHARK_ATTACKS_ROWS_PER_PAGE,
        rowsPerPage
    }
}

/**
 * Set the table-column order
 * @param {*} order 
 */
export function setSharkAttacksOrder(order) {
    return {
        type: SET_SHARK_ATTACKS_ORDER,
        order
    }
}

/**
 * Set the name filter
 * @param {string} name 
 */
export function setSharkAttacksFilterName(name) {    
    return {
        type: SET_SHARK_ATTACKS_FILTERS_NAME,
        name
    }
}

/**
 * Set the filter active flag on/off/both
 * @param {boolean} active 
 */
export function setSharkAttacksFilterActive(active) {
    return {
        type: SET_SHARK_ATTACKS_FILTERS_ACTIVE,
        active
    }
}

/**
 * set the organizationId filter
 * @param {string} organizationId 
 */
export function setSharkAttacksFilterOrganizationId(organizationId) {    
    return {
        type: SET_SHARK_ATTACKS_FILTERS_ORGANIZATION_ID,
        organizationId
    }
}



