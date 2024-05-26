# pg-hub event details
Each event includes a *topic*, *key*, and *value*. The *topic* is an alphanumeric 
string that corresponds directly to a PSQL notification channel. The *key*
is an ordered list of 64-bit signed integers, and the *value* is a
JSON object. The *key* and *value* are encoded into the notification payload.

## example encodings
Originating outbound `notify()` calls to the local **hub** object:
```js
notify('topic0')
notify('topic1', [])
notify('topic2', [1,2,3,4])
notify('topic3', 5, {})
notify('topic4', null, {a:2, b:{d:1, e:'$$'}})
notify('topic5', [1,2,3,4], [5678,910])
notify('topic6', [-9223372036854775808n,9223372036854775807n], 'John Malkovich?')
```
Resulting SQL `notify` statements from the local **hub** object to the PosgreSQL server:
```sql
notify topic0,$$e$$
notify topic1,$$e$$
notify topic2,$$e1,2,3,4$$
notify topic3,$$e5:{}$$
notify topic4,$$e:{a:2, b:{d:1, e:"\u0024\u0024"}}$$
notify topic5,$$e1,2,3,4:[5678,910]$$
notify topic6,$$e-9223372036854775808,9223372036854775807:"John Malkovich?"$$
```

Resulting inbound `notify()` callback hooks from the remote (or local) **hub** object
to its subscribing **consumer** objects:
```js
notify({topic:'topic0', key:[], value:null})
notify({topic:'topic1', key:[], value:null})
notify({topic:'topic2', key:[1n,2n,3n,4n], value:null})
notify({topic:'topic3', key:[5n], value:{})
notify({topic:'topic4', key:[], value:{a:2, b:{d:1, e:'$$'}})
notify({topic:'topic5', key:[1n,2n,3n,4n], value:[5678,910]})
notify({topic:'topic6', key:[-9223372036854775808n,9223372036854775807n], value:'John Malkovich?'})
```
