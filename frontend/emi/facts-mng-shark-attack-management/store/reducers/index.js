import {combineReducers} from 'redux';
import sharkAttacks from './SharkAttacks.reducer';

const reducer = combineReducers({
    sharkAttacks,
});

export default reducer;
