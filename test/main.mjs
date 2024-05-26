'use strict'

import Pg from 'pg';
import {describe, it} from 'mocha';
import config from './config.mjs';
import {default as tests_a} from './testgroup_a.mjs';
import {default as tests_b} from './testgroup_b.mjs';
import {default as tests_c} from './testgroup_c.mjs';

let _pool = new Pg.Pool(config.db);

describe('lifecycle', function () {
    this.timeout(5000);
    it('zero lifespan with no consumers v1', () => {
        return tests_a.lifeZeroNoConsumersV1(_pool)
    });
    it('zero lifespan with no consumers v2', () => {
        return tests_a.lifeZeroNoConsumersV2(_pool)
    });
    it('brief lifespan with no consumers', () => {
        return tests_a.lifeBriefNoConsumers(_pool)
    });
    it('brief lifespan with 256 consumers (v1)', () => {
        return tests_a.lifeBrief256ConsumersV1(_pool)
    });
    it('brief lifespan with 256 consumers (v2)', () => {
        return tests_a.lifeBrief256ConsumersV2(_pool)
    });
    it('brief lifespan with 256 consumer daisychain', () => {
        return tests_a.lifeBrief256ConsumersDaisy(_pool)
    });
});
describe('traffic', function () {
    this.timeout(5000);
    it('evil consumer', () => {
        return tests_b.badConsumer1(_pool);
    });

    it('evil producer', () => {
        return tests_b.badProducer1(_pool);
    });

    it('round-trip shapes', () => {
        return tests_b.roundTripShapes(_pool);
    });
});

describe('system', function () {
    this.timeout(60000);
    it('postgreSQL table dual cache', () => {
        return tests_c.cache(_pool);
    });
});
