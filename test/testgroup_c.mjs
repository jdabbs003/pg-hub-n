import PgHub from "../src/index.mjs";
import assert from 'assert';

const stmt0 =
    'create table test (\n' +
    ' id bigint PRIMARY KEY,\n' +
    ' col1 varchar(40) NOT NULL,\n' +
    ' col2 integer NOT NULL\n' +
    ');'

const stmt1 =
    'create or replace function fun_notify_tab_stm() returns trigger\n' +
    'as $$\n' +
    ' declare key_count integer := 0;\n' +
    ' declare key_list text := \'\';\n' +
    ' declare payload text;\n' +
    ' declare channel text := TG_TABLE_NAME || \'_stm\';\n' +
    ' declare key_max integer = 300;\n' +
    ' begin' +
    '  if (TG_OP = \'DELETE\') then\n' +
    '   select count(id) from old_table into key_count;\n' +
    '   if (key_count < key_max) then\n' +
    '    select string_agg(id::text, \',\') from old_table into key_list;\n' +
    '   end if;\n' +
    '  end if;\n' +

    '  if (TG_OP = \'UPDATE\') then\n' +
    '   select distinct count(id) from ((select id from new_table) union (select id from old_table)) into key_count;\n' +
    '   if (key_count < key_max) then\n' +
    '    select distinct string_agg(id::text, \',\') from ((select id from new_table) union (select id from old_table)) into key_list;\n' +
    '   end if;\n' +
    '  end if;\n' +

    '  if (TG_OP = \'INSERT\') then\n' +
    '   select count(id) from new_table into key_count;\n' +
    '   if (key_count < key_max) then\n' +
    '    select string_agg(id::text, \',\') from new_table into key_list;\n' +
    '   end if;\n' +
    '  end if;\n' +

    '  payload :=' +
    '   \'e\' ||\n' +
    '   key_list ||\n' +
    '   \':{"op":"\' ||\n' +
    '   lower(substring(TG_OP,1,3)) ||\n' +
    '   \'","count":\' ||\n' +
    '   key_count ||\n' +
    '   \'}\';\n' +

    '  perform pg_notify(channel, payload);\n' +
    '  return NULL;\n' +
    '  end;\n' +
    '$$ language plpgsql;'

const stmt2 =
    'create trigger test_notify_stm_ins after insert on test\n' +
    'referencing new table as new_table\n' +
    'for each statement execute function fun_notify_tab_stm();'

const stmt3 =
    'create trigger test_notify_stm_upd after update on test\n' +
    'referencing old table as old_table new table as new_table\n' +
    'for each statement execute function fun_notify_tab_stm();'

const stmt4 =
    'create trigger test_notify_stm_del after delete on test\n' +
    'referencing old table as old_table\n' +
    'for each statement execute function fun_notify_tab_stm();'

const stmt5 =
    'drop table if exists test;'

const stmt6 =
    'drop function if exists fun_notify_tab_stm;'

const setup = [
    stmt5, stmt6, stmt0, stmt1, stmt2, stmt3, stmt4
]

const cleanup = [stmt5, stmt6]

const sleep = (delay) => new Promise((resolve) => setTimeout(resolve, delay))

const runSequence = async (pool, statements) => {
    try {
        for (let i = 0; i != statements.length; ++i) {
            const result = await pool.query(statements[i]);
        }

        return true;
    } catch (err) {
        return false
    }
}

class Cache {
    pool;
    hub;
    consumer;
    rows = new Map();
    promise;
    resolve;
    magic;
    events = [];

    constructor(pool, hub, magic) {
        this.magic = BigInt(magic);
        this.pool = pool;
        this.hub = hub;
        const self = this;
        this.consumer = hub.consumer((event) => self.queueEvent(event));
        this.consumer.subscribe('test_stm');
        this.promise = new Promise((res) => (self.resolve = res));
    }

    close() {
        this.consumer.close();
        this.consumer = null;
    }

    confirm(length) {
        if (this.rows.size !== length) {
            return false;
        }

        const quartile1 = BigInt(length / 4);
        const quartile2 = BigInt(length / 2);
        const quartile3 = BigInt(3 * (length / 4));

        for (let i = 1n; i !== BigInt(length); ++i) {
            const row = this.rows.get(i);

            if (i < quartile1) {
                if (row.col1 !== 'quartile 1') {
                    return false;
                }
            } else if (i < quartile2) {
                if (row.col1 !== 'quartile 2') {
                    return false;
                }
            } else if (i < quartile3) {
                if (row.col1 !== 'quartile 3') {
                    return false;
                }
            } else if (row.col1 !== 'quartile 4') {
                return false;
            }
        }

        return true;
    }

    async queueEvent(event) {
        this.events.push(event);
        if (this.events.length === 1) {
            while (this.events.length !== 0) {
                await this.handleEvent(this.events[0]);
                this.events.shift();
            }
        }
    }

    async handleEvent(event) {
        const pool = this.pool;
        if (event.topic !== null) {
            switch (event.value.op) {
                case 'ins': {
                    for (let i = 0; i !== event.keys.length; ++i) {
                        const result = await pool.query('select * from test where id=$1', [event.keys[i]]);
                        if (result.rowCount === 1) {
                            this.rows.set(event.keys[i], result.rows[0]);
                        }
                    }

                    break;
                }

                case 'upd': {
                    for (let i = 0; i !== event.keys.length; ++i) {
                        const result = await pool.query('select * from test where id=$1', [event.keys[i]]);
                        if (result.rowCount === 1) {
                            this.rows.set(event.keys[i], result.rows[0]);
                        }

                        if (event.keys[i] === this.magic) {
                            this.resolve(true);
                        }
                    }

                    break;
                }

                case 'del': {
                    for (let i = 0; i != event.keys.length; ++i) {
                        this.rows.delete(event.keys[i]);
                    }

                    break;
                }
            }
        }
    }
}

const _cache = async (pool, hub) => {
    const rowCount = 256;
    let cache0 = new Cache(pool, hub, rowCount - 1);
    let cache1 = new Cache(pool, hub, rowCount - 1);

    let rows = new Map();

    for (let i = 0; i != rowCount; ++i) {
        const result = await pool.query('insert into test (id, col1, col2) values ($1,$2,$3)',
            [BigInt(i), 'quartile 4', i % 16]);
    }

    const result0 = await pool.query('update test set col1=$1 where id<$2', ['quartile 3', (rowCount * 3) / 4]);
    const result1 = await pool.query('update test set col1=$1 where id<$2', ['quartile 2', (rowCount * 2) / 4]);
    const result2 = await pool.query('update test set col1=$1 where id<$2', ['quartile 1', (rowCount * 1) / 4]);

    for (let i = rowCount; i < rowCount + 100; ++i) {
        const result = await pool.query('insert into test (id, col1, col2) values ($1,$2,$3)',
            [BigInt(i), 'quartile 5', 0]);
    }

    const result3 = await pool.query('delete from test where id>$1', [rowCount - 1]);

    const result4 = await pool.query('update test set col2=$1 where id=$2', [-1, rowCount - 1]);

    const result5 = await cache0.promise;

    return cache0.confirm(rowCount) && cache1.confirm(rowCount);
}
const test1 = async (pool) => {
    const result1 = await runSequence(pool, setup);
    assert.equal(result1, true);

    const hub = new PgHub.Hub(pool, 5000, null, 'test_hub_0');

    const result2 = await hub.start();
    assert.equal(result2, true);

    const result3 = await _cache(pool, hub);
    assert.equal(result3, true);

    const result4 = await runSequence(pool, cleanup);
    assert.equal(result4, true);

    const result5 = await hub.stop();

    assert.equal(result5, true);
}

const tests = {cache: test1}
export default tests;
