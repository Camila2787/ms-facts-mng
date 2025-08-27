import React from 'react';
import { Redirect } from 'react-router-dom';
import i18n from './i18n'

const auth = ["SHARK_ATTACK_READ"];

export const MicroFrontendConfig = {
    settings: {
        layout: {}
    },
    auth,
    routes: [
        { 
            path: '/shark-attack-mng/shark-attacks/:sharkAttackId/:sharkAttackHandle?',
            component: React.lazy(() => import('./shark-attack/SharkAttack'))
        },
        {
            path: '/shark-attack-mng/shark-attacks',
            component: React.lazy(() => import('./shark-attacks/SharkAttacks'))
        },
        {
            path: '/shark-attack-mng',
            component: () => <Redirect to="/shark-attack-mng/shark-attacks" />
        }
    ],
    navigationConfig: [
        {
            'id': 'settings',
            'type': 'collapse',
            'icon': 'settings',
            'priority': 100,
            children: [{
                'id': 'facts-mng-shark-attack-management',
                'type': 'item',
                'icon': 'business',
                'url': '/shark-attack-mng',
                'priority': 2000,
                auth
            }]
        }
    ],
    i18nLocales: i18n.locales
};

