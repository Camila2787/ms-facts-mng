"use strict";

const Rx = require('rxjs');

const SharkAttackDA = require("./SharkAttackDA");

module.exports = {
  /**
   * Data-Access start workflow
   */
  start$: Rx.concat(SharkAttackDA.start$()),
  /**
   * @returns {SharkAttackDA}
   */
  SharkAttackDA: SharkAttackDA,
};
