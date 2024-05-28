'use strict'

import Pg from 'pg'
import crypto from 'node:crypto';
import PgHub from './src/index.mjs'

const config= {
    db: {
        host: "127.0.0.1",
        port: 5432,
        user: "test_user",
        password: "test_pass",
        database: "test_db",
        keepAliveInitialDelayMillis: 10000,
        keeplive: true,
    }
}

class TestLogger
{
    debug = null;

    info(text)
    {
        console.info('hub: ' + text)
    }

    error(text)
    {
        console.error('hub: ' + test);
    }

    warn(text)
    {
        console.warn('hub: ' + text)
    }
}

class Test1 {
    static Config = class {
        pool;
        logger;
        reconnectTimeMs;
        instances;
        iterations;
    }

    config;
    /** @type {Uint8Array} */
    completion;
    hub;
    topicBase;

    /**
     *
     * @param {Test1.Config} config
     */

    constructor(config) {
        this.config = config;
        this.completion = new Uint8Array(config.iterations * config.instances);
        this.completion.fill(0);
    }

    async runInstance(instance) {
        const self = this;
        const cfg = this.config;

        const timeMax = 60000;
        let resolveComplete = null;
        let complete = new Promise(r => resolveComplete = r);
        let recvCount = 0;
        let sendCount = 0;
        const topicName = this.topicBase + '_' + instance;
        const keyBase =  BigInt('0x' + crypto.randomBytes(15).toString('hex'));

        let consumer = this.hub.consumer((event) => {
            if (event.topic === topicName) {
                const i = (instance * this.config.iterations) + Number(event.keys[0] - keyBase);
                ++self.completion[i];

                if (++recvCount === cfg.iterations) {
                    resolveComplete(true);
                }
            } else {

            }
        });

        consumer.subscribe(topicName);

        setTimeout(() => resolveComplete(false), timeMax)

       this.hub.notify('f', null, {a: () => {return false;}});

        for (sendCount = 0; sendCount < cfg.iterations; ++sendCount) {
            this.hub.notify(topicName, [keyBase + BigInt(sendCount),2,3,4,5], {i: sendCount, j:{a:'$$ $$',b:42,c:[0,2,3,4]}});
        }

        let result = await complete;

        if (result) {
            console.info("Iteration " + instance + " completed successfully");
        } else {
            console.info("Iteration " + instance + " timed out");
        }

        consumer.close();

        return result;
    }

    async run() {
        const cfg = this.config;

        this.hub = new PgHub.Hub(cfg.pool, cfg.reconnectTimeMs, cfg.logger, 'test');
        this.topicBase = "chan_" + Array.from(Array(5), () => Math.floor(Math.random() * 36).toString(36)).join('');

        const started = await this.hub.start();

        const promises = [];
        for (let instance = 0; instance < cfg.instances; ++instance) {
            promises.push(this.runInstance(instance));
        }

        const results = await Promise.all(promises);
        const finalResult1 = (-1 === results.indexOf(false));
        const finalResult2 = (-1 === this.completion.findIndex((value) => {
            return (value !== 1)
        }));

        const stopped = await this.hub.stop();

        return finalResult1 && finalResult2;
    }
}

async function main() {
    let pgPool = new Pg.Pool(config.db);

    {
        let testConfig = new Test1.Config();
        testConfig.instances = 16;
        testConfig.iterations = 16;
        testConfig.pool = pgPool;
        testConfig.reconnectTimeMs = 5000;
        testConfig.logger = new TestLogger()

        const test = new Test1(testConfig);
        const result = await test.run();

        if (result) {
            console.info("Test 1 passed");
        } else {
            console.error("Test 1 failed");
        }
    }
}

await main();