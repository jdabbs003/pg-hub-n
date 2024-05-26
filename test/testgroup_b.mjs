import PgHub from "../src/index.mjs";
import assert from 'assert';
import crypto from 'node:crypto';

const distinguishedString=(obj)=> {
    let seen = new Set();
    JSON.stringify(obj, (key, value) => {
        seen.add(key);
        return value;
    });

    let keys = [];
    seen.forEach((value) => keys.push(value));
    keys.sort();
    const result = JSON.stringify(obj, keys);
    return result;
}

const compareValues = (v0, v1) => {
    return (distinguishedString(v0) === distinguishedString(v1));
}

const compareKeys = (k0, k1) => {
    if ((k0 === null) || (k0 === undefined)) {
        return (k1.length === 0);
    }

    if (!Array.isArray(k0)) {
        k0 = [k0];
    }

    if (k0.length != k1.length) {
        return false;
    }

    if (k0.length === 0) {
        return true;
    }

    try {
        for (let i = 0; i !== k0.length; ++i) {
            if (k1[i] !== BigInt(k0[i])) {
                return false;
            }
        }
    } catch (err) {
        return false;
    }

    return true;
}

const test1 = async (pool) => {
    const consumerCount = 16;
    const eventCount = 16;
    const hub = new PgHub.Hub(pool, 5000, null, 'test_hub_0');
    let resolve1 = null;
    const promise1 = new Promise(res => resolve1 = res)
    let resolve2 = null;
    const promise2 = new Promise(res => resolve2 = res)
    let stopCounter = 0;
    let eventCounter = 0;
    let consumers = [consumerCount];

    for (let i = 0; i != consumerCount; ++i) {
        const consumer = hub.consumer((event) => {
            if (event.topic === 'topic0') {
                assert.equal(event.keys.length, 3);
                assert.equal(event.keys[0], 1n);
                assert.equal(event.value.x, event.value.y);

                let success = false;

                try {
                    delete event.value.x;
                } catch (err) {
                    success = true;
                }

                assert.equal(success, true);

                success = false;
                try {
                    event.value.y = -50;
                } catch (err) {
                    success = true;
                }

                assert.equal(success, true);

                success = false;
                try {
                    event.value.push(0);
                } catch (err) {
                    success = true;
                }

                assert.equal(success, true);

                success = false;
                try {
                    event.keys[0] = 222n;
                } catch (err) {
                    success = true;
                }

                assert.equal(success, true);

                success = false;
                try {
                    event.topic = null;
                } catch (err) {
                    success = true;
                }

                assert.equal(success, true);

                success = false;
                try {
                    event.keys = null;
                } catch (err) {
                    success = true;
                }

                assert.equal(success, true);

                success = false;
                try {
                    event.value = null;
                } catch (err) {
                    success = true;
                }

                assert.equal(success, true);

                if (++eventCounter === (consumerCount * eventCount)) {
                    resolve1(true);
                }
            } else if (event.topic === null) {
                if (event.value.action === 'stop') {
                    if (++stopCounter === consumerCount) {
                        resolve2(true);
                    }
                }
            }
        });

        const result1 = consumer.subscribe(['topic0','()=>{return true;}']);
        assert.equal(result1, false);

        const result2 = consumer.subscribe('topic0');
        assert.equal(result2, true);

        consumers[i] = consumer;
    }

    const result0 = await hub.start();

    assert.equal(result0, true);

    for (let i = 0; i != eventCount; ++i) {
        hub.notify('topic0', [1, 2, 3], {x: i, y: i, z: 100});
    }
    const result1 = await promise1;
    assert.equal(result1, true);

    const results2 = await Promise.all([hub.stop(), promise2]);

    assert.equal(results2[0], true);
    assert.equal(results2[1], true);
}

const test2 = async (pool) => {
    //  const consumerCount = 16;
    const eventMax = 32;
    let eventCount = 0;
    const topic = 't1';
    const hub = new PgHub.Hub(pool, 5000, null, 'test_hub_0');
    let resolve1 = null;
    const promise1 = new Promise(res => resolve1 = res)

    let events = [
        new PgHub.Event(topic, null, null),
        new PgHub.Event(topic, [], null),
        new PgHub.Event(topic, [], {}),
        new PgHub.Event(topic, 10000000000n, {}),
        new PgHub.Event(topic, 10000000001n, true),
        new PgHub.Event(topic, 10000000000n, 'on a dark desert highway'),
        new PgHub.Event(topic, [10000000000n, 10000000000n, 10000000000n], ['{}', '$', 'llm', 'forthright']),
        new PgHub.Event(topic, [10000000000n, 10000000000n, 10000000000n], 12345),
        new PgHub.Event(topic, ['10000000000', 10000000000n, 5000000000n + 5000000000n], ['{}', '$', 'llm', 'forthright']),
        new PgHub.Event(topic, [10000000000n, 10000000000n, 10000000000n], {
            w: {x: 'y'},
            a: 0,
            b: {e: ['fghij', 'klmno'], c: 'd'},
            p: {q: 'rstuv'},
            z: null
        }),
        new PgHub.Event(topic, [0, 10000000000n, 1000000000],
            {
                aaa: [{a: {b: {c: {d: {e: {f: {g: {h: {i: null}}}}}}}}},
                    {j: {k: {l: {m: {n: {o: {p: {q: {r: null}}}}}}}}},
                    {s: {t: {u: {v: {w: {x: {y: {z: {a0: null}}}}}}}}}]
            }
        ),
        new PgHub.Event(topic, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], {hhh: 'abcdefg' + 'hijklmnop' + 'qrstuvw' + 'xyz'}),
    ];

    for (let j = 0; j != 10; ++j) {
        let keys = [];

        for (let i = 0; i < Math.random() * 300; ++i) {
            keys.push(BigInt('0x' + crypto.randomBytes(15).toString('hex')));
        }

        let value = {}
        for (let i = 0; i < Math.random() * 100; ++i) {
            value['a' + i] = Math.floor() * 100000;
        }

        events.push(new PgHub.Event(topic, keys, value));
    }

    let success = true;
    const consumer = hub.consumer((event) => {
        if (event.topic === topic) {
            const ndx = eventCount++ % events.length;
            const sentEvent = events[ndx];

            const result1 = compareValues(sentEvent.value, event.value);
            if (!result1) success = false;

            const result2 = compareKeys(sentEvent.keys, event.keys);
            if (!result2) success = false;

            if (eventCount === eventMax) {
                resolve1(success);
            }
        }
    });

    consumer.subscribe([topic])

    const result0 = await hub.start();
    assert.equal(result0, true);

    for (let i = 0; i != eventMax; ++i) {
        const e = events[i % events.length];
        const result = hub.notify(e.topic, e.keys, e.value);
        assert.equal(result, true);
    }

    const result1 = await promise1;
    assert.equal(result1, true);

    const result2 = await hub.stop();
    assert.equal(result2, true);
    return true;
}
const test3 = async (pool) => {
    //  const consumerCount = 16;
    const eventMax = 32;
    let eventCount = 0;
    const topic = 't3';
    const hub = new PgHub.Hub(pool, 5000, null, 'test_hub_0');
    let resolve1 = null;
    const promise1 = new Promise(res => resolve1 = res)

    let events = [
        new PgHub.Event(topic, null, {a: 1n}),
        new PgHub.Event(topic, ['x','y','z'], {}),
        new PgHub.Event(topic, [1.1,1.2,1.3], {}),
        new PgHub.Event(topic, 1.3, {}),
        new PgHub.Event(topic, [null], {}),
        new PgHub.Event('a-b-c', 10000000001n, true),
        new PgHub.Event('()', 10000000001n, true),
];

    let received = 0;
    const consumer = hub.consumer((event) => {
        if (event.topic) {
            received++;
        }
    });

    consumer.subscribe([topic])

    const result0 = await hub.start();
    assert.equal(result0, true);

    for (let i = 0; i != events.length; ++i) {
        const e = events[i ];
        const result = hub.notify(e.topic, e.keys, e.value);
        assert.equal(result, false);
    }

    const result1 = hub.notify(topic);
    assert.equal(result1, true);

    const attacks = [
        'notify ' + topic + ',$$eaksdjlkjasdlkjasdlkjasd$$',
        'notify '+ topic + ',$$e1,k,,,:{}$$',
        'notify '+ topic + ',$$e1,2,3:{x:1n}$$',
        'notify '+ topic + ',$$e1,2,3:[1n]$$',
        'notify '+ topic + ',$$e1,2,3:[{()=>{return 0;}]$$'
    ];

    for (let i = 0; i != attacks.length; ++i) {
        const result2 = await pool.query(attacks[i]);
    }

    await new Promise((resolve) => setTimeout(resolve, 3000))

    const result3 = await hub.stop();
    assert.equal(result3, true);

    return (received == 1);
}

const tests = {badConsumer1: test1, badProducer1:test3,roundTripShapes: test2};
export default tests;