'use strict'

// pg-hub
// Copyright (c) 2020-2024 James M Dabbs III
// ISC License

class IbEvent {
    consumer;
    event;

    constructor(consumer, event) {
        this.consumer = consumer;
        this.event = event;
    }
}

/**
 * @callback NotifyHook
 * @param {PgHub.Event} event
 * @param {PgHub.Consumer} consumer
 */

/**
 * @callback LogHook
 * @param {string} text
 */

/**
 *
 * @param {string} topic
 * @return {boolean}
 */

const validTopic = (topic) => {
    return !/[^a-zA-Z0-9_]/.test(topic);
}

class FsmInput {
    static startReq = 0;
    static stopReq = 1;
    static queueReq = 2;
    static connectResult = 3;
    static queryResult = 4;
    static timeout = 5;
    static notify = 6;
    static error = 7;

    code;
    param1;
    param2;

    constructor(code, param1, param2) {
        this.code = code;
        this.param1 = param1;
        this.param2 = param2
    }
}

const FsmState = {
    init: 0,
    connecting: 1,
    waiting: 2,
    connected: 3,
    querying: 4,
    stopped: 5
}

const fsmStateText = (state) => {
    switch (state) {
        case FsmState.init:
            return 'init';
        case FsmState.connecting:
            return 'connecting';
        case FsmState.waiting:
            return 'waiting';
        case FsmState.connected:
            return 'connected';
        case FsmState.querying:
            return 'querying';
        case FsmState.stopped:
            return 'stopped';
        default:
            return 'unknown';
    }
}

const IbQState = Object.freeze({
    idle: 0,
    pending: 1,
    working: 2
})

const codec = {
    encode: (key, value) => {
        key = codec.encodeKey(key);

        if (key === undefined) {
            return undefined;
        }

        value = codec.encodeValue(value);

        if (value === undefined) {
            return undefined;
        }

        let payload = 'e';

        if (key) {
            payload += key;
        }

        if (value) {
            payload += ':' + value;
        }

        return payload;
    },

    encodeValue: (value) => {
        if (value === undefined) {
            value = null;
        }

        try {
            value = JSON.stringify(value);
        } catch (err) {
            return undefined;
        }

        return value.replaceAll("$", '\\u0024');
    },

    encodeKey: (key) => {
        if ((key === null) || (key === undefined)) {
            return null;
        }

        try {
            if (!Array.isArray(key)) {
                key = [key];
            }

            let encoding = '';
            for (let i = 0; i !== key.length; ++i) {
                if (i !== 0) {
                    encoding = encoding + ','
                }

                encoding += BigInt(key[i]).toString()
            }

            return encoding;
        } catch (err) {
            return undefined;
        }
    },

    decode: (channel, payload) => {
        if (!payload) {
            return undefined;
        }

        if (payload[0] !== 'e') {
            return undefined;
        }

        if (payload.length === 1) {
            return Object.freeze(new PgHub.Event(channel, emptyArray, null));
        }

        const sepPos = payload.indexOf(':', 1);

        if (sepPos === -1) {
            const key = codec.decodeKey(payload, 1, payload.length);
            if (key === undefined) {
                return undefined;
            }

            return Object.freeze(new PgHub.Event(channel, key, null));
        }

        const key = codec.decodeKey(payload, 1, sepPos);
        const value = codec.decodeValue(payload, sepPos + 1);

        return Object.freeze(new PgHub.Event(channel, key, value));
    },

    decodeValue: (payload, startPos) => {
        try {
            return JSON.parse(payload.substring(startPos), (k, v) => {
                if (typeof v === 'object') {
                    Object.freeze(v);
                }
                return v;
            });
        } catch (err) {
            return undefined;
        }
    },

    decodeKey: (payload, startPos, endPos) => {
        if (startPos !== endPos) {
            try {
                const stringKeys = payload.substring(startPos, endPos).split(',');
                const keys = [stringKeys.length];
                stringKeys.forEach((value, index) => keys[index] = BigInt(value));
                return Object.freeze(keys);
            } catch (err) {
                return undefined;
            }
        }

        return emptyArray;
    }
}

const buildEvent = (topic, keys, value) => {
    return Object.freeze(new PgHub.Event(topic, keys, value));
}

const PgHub = {
    Event: class {
        /** @type {?string} */
        topic;
        /** @type {BigInt[]} */
        keys;
        /** @type {?object} */
        value;

        constructor(topic, keys, value) {
            this.topic = topic;
            this.keys = keys;
            this.value = value;
        }
    },

    Consumer: class {
        /** @type {PgHub.Hub} */
        #hub;
        /** @type {Map<string, Set>} */
        #sets = new Map();
        /** @type {NotifyHook} */
        #hook;

        /**
         *
         * @param {PgHub.Hub} hub
         * @param {NotifyHook} hook
         */

        constructor(hub, hook) {
            this.#hub = hub;
            this.#hook = hook;
        }

        /** @return {boolean} */
        get closed() {
            return (this.#hub === null);
        }

        get _sets() {
            return this.#sets;
        }

        /**
         * @param {string | string[]} topic
         */

        subscribe(topic) {
            if (this.#hub) {
                return this.#hub._consumerSubscribe(this, topic);
            } else {
                return false;
            }
        }

        close() {
            return this.#hub._consumerClose(this);
        }

        _terminate() {
            this.#hub = null;
            this.#sets = null;
        }

        /**
         *
         * @param {PgHub.Event} event
         * @private
         */

        _notify(event) {
            if (this.#hook) {
                this.#hook(event, this);

                if ((event.topic === null) && (event.value.final)) {
                    this.#hook = null;
                    this.#hub = null;
                    this.#sets = null;
                }
            }
        }
    },
    Hub: class {
        static Stats = class {
            listens = 0n;
            sent = 0n;
            received = 0n;
            receivedError = 0n;
            receivedUnroutable = 0n;
            connect = 0n;
            connectFail = 0n;
            connectLoss = 0n;
        }

        /** @type {string} */
        #name;
        /** @type {Map<string, Set>} */
        #topics = new Map();
        /** @type {Set<PgHub.Consumer>} */
        #consumers = new Set();// ConsumerSet(null);
        #pgPool;
        #logger;
        /** @type {number} */
        #reconnectTime;

        /** @type {PgHub.Hub.Stats} */
        #stats = new PgHub.Hub.Stats();

        /** @type {IbEvent[]} */
        #ibQ = [];
        /** @type {number} */
        #ibQ_state = IbQState.idle;

        /** @type {number} */
        #fsmState = FsmState.init;
        #fsmPgClient = null;
        /** @type {Promise} */
        #fsmStopPromise = null;
        /** @type {function} */
        #fsmStopResolve = null;
        /** @type {Promise} */
        #fsmStartPromise = null;
        /** @type {function} */
        #fsmStartResolve = null;
        #fsmTimeout = null;
        /** @type {string[]} */
        #fsmObQ0 = [];
        /** @type {string[]} */
        #fsmObQ1 = [];
        /** @type {string[]} */
        #fsmPendingObQ = null;
        /** @type {BigInt} */
        #fsmClientSeqn = 0n;
        /** @type {BigInt} */
        #fsmEntrancy = 0n;

        /**
         *
         * @param {object} pgPool
         * @param {number} reconnectTime
         * @param {object=null} logger
         * @param {LogHook} logger.debug
         * @param {LogHook} logger.info
         * @param {LogHook} logger.warn
         * @param {LogHook} logger.error
         * @param {string=null} name
         */
        constructor(pgPool, reconnectTime, logger, name) {
            this.#pgPool = pgPool;
            this.#reconnectTime = reconnectTime;
            this.#logger = (logger === undefined) ? null : logger;
            this.#name = (name === undefined) ? null : name;

            let resolve = null;

            this.#fsmStartPromise = new Promise(res => resolve = res);
            this.#fsmStartResolve = resolve;

            this.#fsmStopPromise = new Promise(res => resolve = res);
            this.#fsmStopResolve = resolve;
        }

        get name() {
            return this.#name;
        }

        /** @return {PgHub.Hub.Stats} */
        get stats() {
            return this.#stats;
        }

        get consumerCount() {
            return this.#consumers.size;
        }

        get topicCount() {
            return this.#topics.size;
        }

        /** @param {string} text */
        #logInfo(text) {
            if ((this.#logger) && (this.#logger.info)) {
                this.#logger.info(text);
            }
        }

        /** @param {string} text */
        #logWarn(text) {
            if ((this.#logger) && (this.#logger.warn)) {
                this.#logger.warn(text);
            }
        }

        /** @param {string} text */
        #logDebug(text) {
            if ((this.#logger) && (this.#logger.debug)) {
                this.#logger.debug(text);
            }
        }

        /** @param {string} text */
        #logError(text) {
            if ((this.#logger) && (this.#logger.error)) {
                this.#logger.error(text);
            }
        }

        #processIbQ(deferred) {
            if (deferred) {
                if (this.#ibQ_state !== IbQState.working) {
                    this.#ibQ_state = IbQState.working;
                    while (this.#ibQ.length !== 0) {
                        const ibQ = this.#ibQ;
                        this.#ibQ = [];
                        for (const event of ibQ) {
                            event.consumer._notify(event.event);
                        }
                    }

                    this.#ibQ_state = IbQState.idle;
                }
            } else {
                if ((this.#ibQ.length !== 0) && (this.#ibQ_state === IbQState.idle)) {
                    this.#ibQ_state = IbQState.pending;
                    const self = this;
                    setImmediate(() => self.#processIbQ(true));
                }
            }
        }

        /**
         *
         * @param {string} topic
         * @param {?BigInt[]} key
         * @param {?object} value
         * @return {boolean}
         */

        notify(topic, key, value) {
            if (!this.#fsmHasStarted()) {
                return false;
            }

            if (!validTopic(topic)) {
                return false;
            }

            const payload = codec.encode(key, value);

            if (payload === undefined) {
                return false;
            }

            this.#fsmQueueNotify(topic, payload);
            return true;
        }

        /** @return {Promise<boolean>} */
        start() {
            this.#fsm(new FsmInput(FsmInput.startReq));
            return this.#fsmStartPromise;
        }

        /** @return {Promise<boolean>} */
        stop() {
            this.#fsm(new FsmInput(FsmInput.stopReq));
            return this.#fsmStopPromise;
        }

        /**
         *
         * @param {NotifyHook} hook
         * @return {PgHub.Consumer}
         */
        consumer(hook) {
            let result = null;

            if (this.#fsmState !== FsmState.stopped) {
                this.#logDebug('creating new consumer');
                const newConsumer = new PgHub.Consumer(this, hook);
                this.#consumers.add(newConsumer);
                return newConsumer;
            }

            return result;
        }

        //
        // Friend methods for Consumer class
        //

        /**
         *
         * @param {PgHub.Consumer} consumer
         * @param {string | string[]} topics
         * @private
         */
        _consumerSubscribe(consumer, topics) {
            if (this.#fsmState === FsmState.stopped) {
                return false;
            }

            if (!Array.isArray(topics)) {
                topics = [topics];
            }

            const sets = consumer._sets;
            let topicCreated = false;

            for (const topic of topics) {
                if (!validTopic(topic)) {
                    return false;
                }
            }

            for (const topic of topics) {
                if (!sets.get(topic)) {
                    let set = this.#topics.get(topic);

                    if (!set) {
                        this.#logInfo('topic "' + topic + '" opened');
                        set = new Set();//ConsumerSet(topics[i]);
                        this.#topics.set(topic, set);
                        topicCreated = true;
                    } else {
                        topicCreated = false;
                    }

                    this.#logDebug('subscription to topic "' + topic + '" started');

                    sets.set(topic, set);
                    set.add(consumer);

                    if (topicCreated) {
                        this.#fsmQueueListen(topic, true);
                    }

                    const event = buildEvent(null, emptyArray, Object.freeze({
                        action: 'subscribe',
                        topic: topic,
                        final: false
                    }));

                    this.#ibQ.push(new IbEvent(consumer, event));
                }
            }

            this.#processIbQ();
            return true;
        }

        /**
         *
         * @param {PgHub.Consumer} consumer
         * @private
         */

        _consumerClose(consumer) {
            const sets = consumer._sets;
            this.#ibQ.push(new IbEvent(consumer, eventClose));

            consumer._terminate();
            this.#consumers.delete(consumer);

            for (const [topic, set] of sets) {
                this.#logDebug('subscription to topic "' + topic + '" ended');
                set.delete(consumer);

                if (set.size === 0) {
                    this.#logInfo('topic "' + topic + '" closed');
                    this.#topics.delete(topic);
                    this.#fsmQueueListen(topic, false);
                }
            }

            this.#processIbQ();
        }

        #fsmQueueListen(channel, listen) {
            this.#logDebug((listen ? 'listen' : 'unlisten') + ' queued for channel "' + channel + '"');
            this.#fsmObQ0.push((listen ? 'listen ' : 'unlisten ') + channel);
            this.#fsm(new FsmInput(FsmInput.queueReq));
        }

        #fsmQueueNotify(channel, payload) {
            this.#logDebug('notify queued for channel "' + channel + '"');
            this.#fsmObQ1.push('notify ' + channel + ',$$' + payload + '$$;');
            this.#fsm(new FsmInput(FsmInput.queueReq));
        }

        #fsmHasStarted() {
            const s = this.#fsmState;

            return (((s === FsmState.connecting) && (this.#fsmStartResolve !== null)) ||
                (s === FsmState.connected) ||
                (s === FsmState.querying));
        }

        #fsmChangeState(newState) {
            if (newState !== this.#fsmState) {
                if (newState === FsmState.stopped) {
                    if (this.#fsmStartResolve !== null) {
                        this.#fsmStartResolve(false);
                        this.#fsmStartResolve = null;
                    }

                    this.#fsmStopResolve(true);
                    this.#fsmStopResolve = null;

                    if (this.#fsmPgClient !== null) {
                        this.#fsmPgClient.release(true);
                        this.#fsmPgClient = null;
                    }

                    if (this.#fsmTimeout !== null) {
                        clearTimeout(this.#fsmTimeout);
                        this.#fsmTimeout = null;
                    }

                    for (const consumer of this.#consumers) {
                        this.#ibQ.push(new IbEvent(consumer, eventStop));
                    }

                    for (const consumer of this.#consumers) {
                        consumer._terminate();
                    }

                    this.#consumers.clear();

                    for (const [topic, set] of this.#topics) {
                        set.clear();
                    }

                    this.#topics = null;
                    this.#pgPool = null;
                }

                this.#logDebug('state change ' + fsmStateText(this.#fsmState) + ' -> ' + fsmStateText(newState));
                this.#fsmState = newState;
            } else {
                this.#logError('state change ' + fsmStateText(this.#fsmState) + ' -> ' + fsmStateText(newState) + ' (same)');
            }
        }

        #fsmExecNotify(data) {
            this.#logDebug('pg notify');
            ++this.#stats.received;

            if (this.#topics === null) {
                console.log('lkjlkj ' + this.#fsmPgClient ? "true" : "false");
                console.log('lkjlkj ' + fsmStateText(this.#fsmState));
                console.log('lkjlkj ' + data.channel);
            }
            const topic = this.#topics.get(data.channel);

            if (topic === undefined) {
                this.#stats.receivedUnroutable++;
                this.#logInfo('unroutable event from ' + data.channel);
            } else {
                const event = codec.decode(data.channel, data.payload);

                if (event === undefined) {
                    this.#stats.receivedError++;
                    this.#logWarn('invalid event from ' + data.channel);
                } else {
                    for (const consumer of topic) {
                        this.#ibQ.push(new IbEvent(consumer, event));
                    }
                }
            }
        }

        #fsmExecQuery() {
            const self = this;

            this.#fsmPendingObQ = null;
            if (this.#fsmObQ0.length !== 0) {
                this.#fsmPendingObQ = this.#fsmObQ0;
            } else if (this.#fsmObQ1.length !== 0) {
                this.#fsmPendingObQ = this.#fsmObQ1;
            }

            if (this.#fsmPendingObQ !== null) {
                this.#fsmPgClient.query(this.#fsmPendingObQ[0], (err, res) => self.#fsm(new FsmInput(FsmInput.queryResult, err, res)));
                return true;
            } else {
                return false;
            }
        }

        /**
         *
         * @param {FsmInput} fsmInput
         */

        #fsm(input) {
            const self = this;

            if (this.#fsmEntrancy !== 0n) {
                this.#logError("fsm reentrancy");
            }

            ++this.#fsmEntrancy;

            if ((input.code === FsmInput.error) && (input.param2 !== this.#fsmClientSeqn)) {
                this.#logInfo("ignored late error from previous connection");
                --this.#fsmEntrancy;
                return;
            }

            if ((input.code === FsmInput.notify) && (input.param2 !== this.#fsmClientSeqn)) {
                this.#logInfo("ignored late notify from previous connection");
                --this.#fsmEntrancy;
                return;
            }

            switch (this.#fsmState) {
                case FsmState.init: {
                    switch (input.code) {
                        case FsmInput.startReq: {
                            this.#fsmChangeState(FsmState.connecting);
                            this.#fsmClientSeqn++
                            this.#pgPool.connect((err, res) => {
                                self.#fsm(new FsmInput(FsmInput.connectResult, err, res))
                            });
                            break;
                        }

                        case FsmInput.stopReq: {
                            this.#fsmChangeState(FsmState.stopped);
                            break;
                        }

                        case FsmInput.queueReq: {
                            break;
                        }

                        default: {
                            this.#logError('unexpected fsm input');
                            break;
                        }
                    }

                    break;
                }
                case FsmState.connecting: {
                    switch (input.code) {
                        case FsmInput.startReq:
                        case FsmInput.queueReq: {
                            break;
                        }

                        case FsmInput.stopReq: {
                            this.#fsmChangeState(FsmState.stopped);
                            break;
                        }

                        case FsmInput.connectResult: {
                            if (input.param2) {
                                this.#fsmClientSeqn++
                                const fsmConnectID = this.#fsmClientSeqn;
                                this.#fsmPgClient = input.param2;
                                this.#fsmPgClient.on('error', err => this.#fsm(new FsmInput(FsmInput.error, err, fsmConnectID)));
                                this.#fsmPgClient.on('notification', data => this.#fsm(new FsmInput(FsmInput.notify, data, fsmConnectID)));

                                this.#fsmObQ0 = ['unlisten *;'];
                                for (const [topic] of this.#topics) {
                                    this.#fsmObQ0.push('listen ' + topic);
                                }

                                if (this.#fsmStartResolve != null) {
                                    this.#fsmStartResolve(true);
                                    this.#fsmStartResolve = null;

                                    for (const consumer of this.#consumers) {
                                        this.#ibQ.push(new IbEvent(consumer, eventConnect));
                                    }
                                } else {
                                    for (const consumer of this.#consumers) {
                                        this.#ibQ.push(new IbEvent(consumer, eventReconnect));
                                    }
                                }

                                ++this.stats.connect;
                                if (this.#fsmExecQuery()) {
                                    this.#fsmChangeState(FsmState.querying);
                                } else {
                                    this.#fsmChangeState(FsmState.connected);
                                }
                            } else {
                                ++this.stats.connectFail;
                                this.#fsmState = FsmState.waiting;
                                this.#fsmTimeout = setTimeout(() => self.#fsm(new FsmInput(FsmInput.timeout)), this.#reconnectTime);
                            }

                            break;
                        }

                        default: {
                            this.#logError('unexpected fsm input');
                            break;
                        }
                    }

                    break;
                }

                case FsmState.waiting: {
                    switch (input.code) {
                        case FsmInput.queueReq:
                        case FsmInput.startReq: {
                            break;
                        }

                        case FsmInput.stopReq: {
                            clearTimeout(this.#fsmTimeout);
                            this.#fsmTimeout = null;
                            this.#fsmChangeState(FsmState.stopped);
                            break;
                        }

                        case FsmInput.timeout: {
                            this.#fsmTimeout = null;
                            this.#fsmChangeState(FsmState.connecting);
                            this.#fsmClientSeqn++
                            this.#pgPool.connect((err, res) => {
                                self.#fsm(new FsmInput(FsmInput.connectResult, err, res))
                            });
                            break;
                        }

                        default: {
                            this.#logError('unexpected fsm input');
                            break;
                        }
                    }

                    break;
                }

                case FsmState.connected: {
                    switch (input.code) {
                        case FsmInput.startReq: {
                            break;
                        }

                        case FsmInput.stopReq: {
                            this.#fsmChangeState(FsmState.stopped);
                            break;
                        }

                        case FsmInput.queueReq: {
                            if (this.#fsmExecQuery()) {
                                this.#fsmChangeState(FsmState.querying);
                            }

                            break;
                        }

                        case FsmInput.notify: {
                            this.#fsmExecNotify(input.param1);
                            break;
                        }

                        case FsmInput.error: {
                            this.#fsmPgClient.release(true);
                            this.#fsmPgClient = null;
                            ++this.stats.connectLoss;

                            for (const consumer of this.#consumers) {
                                this.#ibQ.push(new IbEvent(consumer, eventDisconnect));
                            }

                            this.#fsmChangeState(FsmState.connecting);
                            this.#fsmClientSeqn++
                            this.#pgPool.connect((err, res) => {
                                self.#fsm(new FsmInput(FsmInput.connectResult, err, res))
                            });
                            break;
                        }

                        default: {
                            this.#logError('unexpected fsm input');
                            break;
                        }
                    }

                    break;
                }

                case FsmState.querying: {
                    switch (input.code) {
                        case FsmInput.queueReq:
                        case FsmInput.startReq: {
                            break;
                        }

                        case FsmInput.stopReq: {
                            this.#fsmChangeState(FsmState.stopped);
                            break;
                        }

                        case FsmInput.queryResult: {
                            if (input.param2) {
                                if (this.#fsmPendingObQ === this.#fsmObQ0) {
                                    ++this.stats.listens;
                                } else {
                                    ++this.stats.sent;
                                }

                                this.#fsmPendingObQ.shift();
                                this.#fsmPendingObQ = null;
                                if (!this.#fsmExecQuery()) {
                                    this.#fsmChangeState(FsmState.connected);
                                }
                            } else {
                                this.#fsmPendingObQ = null;
                                this.#fsmPgClient.release(true);
                                this.#fsmPgClient = null;
                                ++this.stats.connectLoss;
                                for (const consumer of this.#consumers) {
                                    this.#ibQ.push(new IbEvent(consumer, eventDisconnect));
                                }
                                this.#fsmChangeState(FsmState.connecting);
                                this.#fsmClientSeqn++;
                                this.#pgPool.connect((err, res) => {
                                    self.#fsm(new FsmInput(FsmInput.connectResult, err, res))
                                });
                            }

                            break;
                        }

                        case FsmInput.notify: {
                            this.#fsmExecNotify(input.param1);
                            break;
                        }

                        default: {
                            this.#logError('unexpected fsm input');
                            break;
                        }
                    }

                    break;
                }

                case FsmState.stopped: {
                    switch (input.code) {
                        case FsmInput.connectResult: {
                            if (input.param2) {
                                input.param2.release(true);
                            }

                            break;
                        }

                        default: {
                            break;
                        }
                    }
                }
            }

            this.#processIbQ();
            --this.#fsmEntrancy;
        }
    }
}

const emptyArray = Object.freeze([]);

const eventConnect = buildEvent(null, emptyArray, Object.freeze({action: 'connect', final: false}));
const eventReconnect = buildEvent(null, emptyArray, Object.freeze({action: 'reconnect', final: false}));
const eventDisconnect = buildEvent(null, emptyArray, Object.freeze({action: 'disconnect', final: false}));
const eventClose = buildEvent(null, emptyArray, Object.freeze({action: 'close', final: true}));
const eventStop = buildEvent(null, emptyArray, Object.freeze({action: 'stop', final: true}));

export default PgHub;