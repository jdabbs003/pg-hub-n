import PgHub from "../src/index.mjs";
import assert from 'assert';

const test0 = async (pool) => {
    const hub = new PgHub.Hub(pool, 5000, null, 'test_hub_0');
    const promise1 = hub.stop();
    const promise2 = hub.start();
    const results = await Promise.all([promise1,promise2]);
    assert.equal(results[0], true);
    assert.equal(results[1], false);
}

const test1 = async (pool) => {
    const hub = new PgHub.Hub(pool, 5000, null, 'test_hub_0');
    const promise1 = hub.start();
    const promise2 = hub.stop();
    const results = await Promise.all([promise1,promise2]);
    assert.equal(results[0], false);
    assert.equal(results[1], true);
}

const test2 = async (pool) => {
    const hub = new PgHub.Hub(pool, 5000, null, 'test_hub_0');
    const result1 = await hub.start();
    const result2 = await hub.stop();
    assert.equal(result1, true);
    assert.equal(result2, true);
}

const test3 = async (pool) => {
    const hub = new PgHub.Hub(pool, 5000, null, 'test_hub_0');
    let completion = new Uint8Array(256);
    completion.fill(0);
    let consumers = []

    let fail = false;
    let completionCount = 0;
    let resolve = {resolve: null};
    let promise = new Promise(res => resolve.resolve = res);

    for (let i = 0; i !== 256; ++i) {
        consumers[i] = hub.consumer((event) => {
            if (event.value.action === 'connect') {
                ++completion[i];
            }
            else if (event.value.action === 'stop') {
                --completion[i];
            }
            else {
                fail = true;
                resolve = false;
            }

            if (++completionCount === 256) {
                resolve.resolve(true);
            }
        });
    }

    assert.equal(hub.consumerCount, 256);

    const result0 = await hub.start();

    const result1 = await promise;

    const finalResult1 = (-1 === completion.findIndex((value) => {
        return (value !== 1);
    }));

    completionCount = 0;
    promise = new Promise(res => resolve.resolve = res);

    const result2 = await Promise.all([hub.stop(), promise]);

    const finalResult2 = (-1 === completion.findIndex((value) => {
        return (value !== 0);
    }));

    assert.equal(result0, true);
    assert.equal(hub.consumerCount, 0);
    assert.equal(result1, true);
    assert.equal(finalResult1, true);
    assert.equal(result2[0], true);
    assert.equal(result2[1], true);
    assert.equal(finalResult2, true);
}

const test4 = async (pool) => {
    const hub = new PgHub.Hub(pool, 5000, null, 'test_hub_0');
    let completion = new Uint8Array(256);
    completion.fill(0);
    let consumers = []

    let fail = false;
    let completionCount = 0;
    let resolve = {resolve: null};
    let promise = new Promise(res => resolve.resolve = res);

    for (let i = 0; i !== 256; ++i) {
        consumers[i] = hub.consumer((event) => {
            if (event.value.action === 'connect')
                ++completion[i];
            else if (event.value.action === 'close')
                --completion[i];
            else {
                fail = true;
                resolve.resolve(true);
            }
            if (++completionCount === 256) {
                resolve.resolve(true);
            }
        });
    }

    assert.equal(hub.consumerCount, 256);

    const result0 = await hub.start();

    const result1 = await promise;

    const finalResult1 = (-1 === completion.findIndex((value) => {
        return (value !== 1);
    }));

    completionCount = 0;
    promise = new Promise(res => resolve.resolve = res);

    consumers.forEach(consumer => consumer.close());

    const result2 = await Promise.all([hub.stop(), promise]);

    assert.equal(hub.consumerCount, 0);

    const finalResult2 = (-1 === completion.findIndex((value) => {
        return (value !== 0);
    }));

    assert.equal(result0, true);
    assert.equal(hub.consumerCount, 0);
    assert.equal(result1, true);
    assert.equal(finalResult1, true);
    assert.equal(result2[0], true);
    assert.equal(result2[1], true);
    assert.equal(finalResult2, true);
}

const test5 = async (pool) => {
    const hub = new PgHub.Hub(pool, 5000, null, 'test_hub_0');
    let completion = new Uint8Array(256);
    completion.fill(0);
    let consumers = []

    let fail = false;
    let subscribeCount = 0;
    let closeCount = 0;
    let resolve1 = null;//{resolve: null};
    let promise1 = new Promise(res => resolve1 = res);

    let hook = (event, this_c) => {
        if (event.value.action === 'subscribe') {
            if (subscribeCount++ < 256) {
                const next_c = hub.consumer(hook);
                next_c.subscribe('test_topic_0');
                this_c.close();
            }
        }

        if (event.value.action === 'close') {
            closeCount++
        }

        if (closeCount === 256) {
            resolve1(subscribeCount === 257);
        }
    }

    const promise0 = hub.start();
    const consumer = hub.consumer(hook);
    const result0 = consumer.subscribe('test_topic_0');
    assert.equal(result0, true);

    const result1 = await Promise.all([promise0, promise1]);

    const result2 = await hub.stop();

    assert.equal(result1[0], true);
    assert.equal(result1[1], true);
    assert.equal(result2, true);
}

const tests = {
    lifeZeroNoConsumersV1: test0,
    lifeZeroNoConsumersV2: test1,
    lifeBriefNoConsumers: test2,
    lifeBrief256ConsumersV1: test3,
    lifeBrief256ConsumersV2: test4,
    lifeBrief256ConsumersDaisy: test5
};
export default tests;