import { gql } from 'apollo-boost';

export const FactsMngSharkAttackListing = (variables) => ({
    query: gql`
            query FactsMngSharkAttackListing($filterInput:FactsMngSharkAttackFilterInput ,$paginationInput:FactsMngSharkAttackPaginationInput,$sortInput:FactsMngSharkAttackSortInput){
                FactsMngSharkAttackListing(filterInput:$filterInput,paginationInput:$paginationInput,sortInput:$sortInput){
                    listing{
                       id,name,active,
                    },
                    queryTotalResultCount
                }
            }`,
    variables,
    fetchPolicy: 'network-only',
})

export const FactsMngSharkAttack = (variables) => ({
    query: gql`
            query FactsMngSharkAttack($id: ID!, $organizationId: String!){
                FactsMngSharkAttack(id:$id, organizationId:$organizationId){
                    id,name,description,active,organizationId,
                    metadata{ createdBy, createdAt, updatedBy, updatedAt }
                }
            }`,
    variables,
    fetchPolicy: 'network-only',
})


export const FactsMngCreateSharkAttack = (variables) => ({
    mutation: gql`
            mutation  FactsMngCreateSharkAttack($input: FactsMngSharkAttackInput!){
                FactsMngCreateSharkAttack(input: $input){
                    id,name,description,active,organizationId,
                    metadata{ createdBy, createdAt, updatedBy, updatedAt }
                }
            }`,
    variables
})

export const FactsMngDeleteSharkAttack = (variables) => ({
    mutation: gql`
            mutation FactsMngSharkAttackListing($ids: [ID]!){
                FactsMngDeleteSharkAttacks(ids: $ids){
                    code,message
                }
            }`,
    variables
})

export const FactsMngUpdateSharkAttack = (variables) => ({
    mutation: gql`
            ,mutation  FactsMngUpdateSharkAttack($id: ID!,$input: FactsMngSharkAttackInput!, $merge: Boolean!){
                FactsMngUpdateSharkAttack(id:$id, input: $input, merge:$merge ){
                    id,organizationId,name,description,active
                }
            }`,
    variables
})

export const onFactsMngSharkAttackModified = (variables) => ([
    gql`subscription onFactsMngSharkAttackModified($id:ID!){
            FactsMngSharkAttackModified(id:$id){    
                id,organizationId,name,description,active,
                metadata{ createdBy, createdAt, updatedBy, updatedAt }
            }
    }`,
    { variables }
])