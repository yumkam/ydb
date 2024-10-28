import pytest
import os
import json
import sys
import random
import base64
import logging
import time
import hashlib
from collections import Counter
from operator import itemgetter

import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1

from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

from ydb.library.yql.providers.generic.connector.tests.utils.one_time_waiter import OneTimeWaiter
from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind

import conftest


DEBUG = 1
XD = 0
WITH_CHECKPOINTS = 1


def ResequenceId(messages, field="id"):
    res = []
    i = 1
    for pair in messages:
        rpair = []
        for it in pair:
            src = json.loads(it)
            if field in src:
                src[field] = i
            rpair += [json.dumps(src)]
        res += [tuple(rpair)]
        i += 1
    return res


def RandomizeMessage(messages, field='message', key='uid', header='Message', biglen=1000):
    res = []
    random.seed(0)  # we want fixed seed
    for pair in messages:
        rpair = []
        r = random.randint(1, 4)
        if r > 3:
            field_val = str(base64.b64encode(random.randbytes(biglen * 6 // 8)), 'utf-8')
            key_val = None
        else:
            field_val = header + str(r)
            key_val = r
            if r == 1:
                key_val = None
        for it in pair:
            src = json.loads(it)
            if field in src:
                src[field] = field_val
            if key in src:
                src[key] = key_val
            rpair += [json.dumps(src)]
        res += [tuple(rpair)]
    return res


def RandomizeDBX(messages, keylen=16):
    # '{"id":1,"age":"foobar","key":"Message5"}',
    # '{"name":null,"id":123,"age":456,"key":null}',
    res = []
    random.seed(0)  # we want fixed seed
    for pair in messages:
        Id = random.randint(0, 1000000)
        Age = random.randint(0, 64)
        Key = str(base64.b64encode(random.randbytes(keylen * 6 // 8)), 'utf-8')
        if Age > 31:
            Age = Id % 31
        Uid = None
        Name = None
        if (Age == Id % 31) and Id < 100000:
            Name = f'Message{Id % 1000}'
            Uid = Id
        rpair = []
        for it in pair:
            src = json.loads(it)
            if 'id' in src:
                src['id'] = Id
            if 'age' in src:
                src['age'] = Age
            if 'name' in src:
                src['name'] = Name
            if 'key' in src:
                src['key'] = Key
            if 'uid' in src:
                src['uid'] = Uid
            rpair += [json.dumps(src)]
        res += [tuple(rpair)]
    return res


def RandomizeDBY(messages, keylen=16):
    # '{"id":1,"age":"foobar","key":"Message5"}',
    # '{"name":null,"id":123,"age":456,"key":null}',
    res = []
    random.seed(0)  # we want fixed seed
    for pair in messages:
        Id = random.randint(0, 10000)
        Id = (Id * 124151351) % 1900000043
        Key = str(base64.b64encode(random.randbytes(keylen * 6 // 8)), 'utf-8')
        Hash = f'hash{Id:028}'
        Age = Id % 31
        Uid = None
        Uhash = None
        Uage = None
        if Id < 10000000:
            Uid = Id
            Uage = Id % 31
            Uhash = Hash
        rpair = []
        for it in pair:
            src = json.loads(it)
            if 'id' in src:
                src['id'] = Id
            if 'uage' in src:
                src['uage'] = Uage
            if 'age' in src:
                src['age'] = Age
            if 'uhash' in src:
                src['uhash'] = Uhash
            if 'hash' in src:
                src['hash'] = Hash
            if 'key' in src:
                src['key'] = Key
            if 'uid' in src:
                src['uid'] = Uid
            rpair += [json.dumps(src)]
        res += [tuple(rpair)]
    return res


def RandomizeDBH(messages, keylen=16):
    # '{"id":1,"age":"foobar","key":"Message5"}',
    # '{"name":null,"id":123,"age":456,"key":null}',
    res = []
    random.seed(0)  # we want fixed seed
    for pair in messages:
        Id = random.randint(0, 10000)
        Id = (Id * 124151351) % 19000043
        Key = str(base64.b64encode(random.randbytes(keylen * 6 // 8)), 'utf-8')
        Hash = hashlib.sha256(bytes(str(Id), 'utf-8')).hexdigest()
        Age = Id % 31
        Uid = None
        Uhash = None
        Uage = None
        if Id < 10000000:
            Uid = Id
            Uage = Id % 31
            Uhash = Hash
        rpair = []
        for it in pair:
            src = json.loads(it)
            if 'id' in src:
                src['id'] = Id
            if 'uage' in src:
                src['uage'] = Uage
            if 'age' in src:
                src['age'] = Age
            if 'uhash' in src:
                src['uhash'] = Uhash
            if 'hash' in src:
                src['hash'] = Hash
            if 'key' in src:
                src['key'] = Key
            if 'uid' in src:
                src['uid'] = Uid
            rpair += [json.dumps(src)]
        res += [tuple(rpair)]
    return res


def RandomizeBIG(messages, keylen=16):
    res = []
    dic = {}
    random.seed(0)
    biglen = int(1*1000*1000//10)
    for i in range(256):  # "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvwxyz" + "0123456789+/":
        rb = random.randbytes(biglen * 6 // 8)
        dic[i] = str(base64.b64encode(rb), 'utf-8')

    random.seed(0)  # we want fixed seed
    for pair in messages:
        Id = random.randint(0, 256*256*256 - 1)
        Prefix = Id & 255
        # Id = (Id * 124151351) % 19000043
        bx = bytes([Id & 255, (Id >> 8) & 255, (Id >> 16) & 255]) + random.randbytes(keylen * 6 // 8 - 3)
        Ev = str(base64.b64encode(bx), 'utf-8')
        Pos = Id % (len(dic[Prefix]) - 4)
        Uev = dic[Prefix][Pos:Pos + 4]
        print((Id, Prefix, Pos, Ev, Uev), file=sys.stderr)
        rpair = []
        for it in pair:
            src = json.loads(it)
            if 'ev' in src:
                src['ev'] = Ev
            if 'xx' in src:
                src['xx'] = Uev
            rpair += [json.dumps(src)]
        res += [tuple(rpair)]
    return res


def freeze(json):
    t = type(json)
    if t == dict:
        return frozenset(sorted((k, freeze(v)) for k, v in json.items()))
    if t == list:
        return tuple(map(freeze, json))
    return json


TESTCASES = [
    # 0
    (
        R'''
            $input = SELECT * FROM myyds.`{input_topic}`;

            $enriched = select
                            e.Data as data, u.id as lookup
                from
                    $input as e
                left join {streamlookup} ydb_conn_{table_name}.{table_name} as u
                on(e.Data = u.data)
            ;

            insert into myyds.`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        [
            ('ydb10', '{"data":"ydb10","lookup":1}'),
            ('ydb20', '{"data":"ydb20","lookup":2}'),
            ('ydb30', '{"data":"ydb30","lookup":3}'),
            ('ydb40', '{"data":"ydb40","lookup":null}'),
            ('ydb50', '{"data":"ydb50","lookup":null}'),
            ('ydb10', '{"data":"ydb10","lookup":1}'),
            ('ydb20', '{"data":"ydb20","lookup":2}'),
            ('ydb30', '{"data":"ydb30","lookup":3}'),
            ('ydb40', '{"data":"ydb40","lookup":null}'),
            ('ydb50', '{"data":"ydb50","lookup":null}'),
        ]
        * 10,
    ),
    # 1
    (
        R'''
            $input = SELECT * FROM myyds.`{input_topic}`;

            $enriched = select
                            e.Data as data, CAST(e.Data AS Int32) as id, u.data as lookup
                from
                    $input as e
                left join {streamlookup} ydb_conn_{table_name}.{table_name} as u
                on(CAST(e.Data AS Int32) = u.id)
            ;

            insert into myyds.`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        [
            ('1', '{"data":"1","id":1,"lookup":"ydb10"}'),
            ('2', '{"data":"2","id":2,"lookup":"ydb20"}'),
            ('3', '{"data":"3","id":3,"lookup":"ydb30"}'),
            ('4', '{"data":"4","id":4,"lookup":null}'),
            ('5', '{"data":"5","id":5,"lookup":null}'),
            ('1', '{"data":"1","id":1,"lookup":"ydb10"}'),
            ('2', '{"data":"2","id":2,"lookup":"ydb20"}'),
            ('3', '{"data":"3","id":3,"lookup":"ydb30"}'),
            ('4', '{"data":"4","id":4,"lookup":null}'),
            ('5', '{"data":"5","id":5,"lookup":null}'),
        ]
        * 3,
    ),
    # 2
    (
        R'''
            $input = SELECT * FROM myyds.`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            id Int32,
                            user Int32,
                        )
                    )            ;

            $enriched = select e.id as id,
                            e.user as user_id,
                            u.data as lookup
                from
                    $input as e
                left join {streamlookup} ydb_conn_{table_name}.{table_name} as u
                on(e.user = u.id)
            ;

            insert into myyds.`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                ('{"id":3,"user":5}', '{"id":3,"user_id":5,"lookup":null}'),
                ('{"id":9,"user":3}', '{"id":9,"user_id":3,"lookup":"ydb30"}'),
                ('{"id":2,"user":2}', '{"id":2,"user_id":2,"lookup":"ydb20"}'),
                ('{"id":1,"user":1}', '{"id":1,"user_id":1,"lookup":"ydb10"}'),
                ('{"id":10,"user":null}', '{"id":10,"user_id":null,"lookup":null}'),
                ('{"id":4,"user":3}', '{"id":4,"user_id":3,"lookup":"ydb30"}'),
                ('{"id":5,"user":3}', '{"id":5,"user_id":3,"lookup":"ydb30"}'),
                ('{"id":6,"user":1}', '{"id":6,"user_id":1,"lookup":"ydb10"}'),
                ('{"id":7,"user":2}', '{"id":7,"user_id":2,"lookup":"ydb20"}'),
            ]
            * 20
        ),
    ),
    # 3
    (
        R'''
            $input = SELECT * FROM myyds.`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            id Int32,
                            ts String,
                            ev_type String,
                            user Int32,
                        )
                    )            ;

            $formatTime = DateTime::Format("%H:%M:%S");

            $enriched = select e.id as id,
                            $formatTime(DateTime::ParseIso8601(e.ts)) as ts,
                            e.user as user_id,
                            u.data as lookup
                from
                    $input as e
                left join {streamlookup} ydb_conn_{table_name}.{table_name} as u
                on(e.user = u.id)
            ;

            insert into myyds.`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                (
                    '{"id":2,"ts":"20240701T113344","ev_type":"foo1","user":2}',
                    '{"id":2,"ts":"11:33:44","user_id":2,"lookup":"ydb20"}',
                ),
                (
                    '{"id":1,"ts":"20240701T112233","ev_type":"foo2","user":1}',
                    '{"id":1,"ts":"11:22:33","user_id":1,"lookup":"ydb10"}',
                ),
                (
                    '{"id":3,"ts":"20240701T113355","ev_type":"foo3","user":5}',
                    '{"id":3,"ts":"11:33:55","user_id":5,"lookup":null}',
                ),
                (
                    '{"id":4,"ts":"20240701T113356","ev_type":"foo4","user":3}',
                    '{"id":4,"ts":"11:33:56","user_id":3,"lookup":"ydb30"}',
                ),
                (
                    '{"id":5,"ts":"20240701T113357","ev_type":"foo5","user":3}',
                    '{"id":5,"ts":"11:33:57","user_id":3,"lookup":"ydb30"}',
                ),
                (
                    '{"id":6,"ts":"20240701T112238","ev_type":"foo6","user":1}',
                    '{"id":6,"ts":"11:22:38","user_id":1,"lookup":"ydb10"}',
                ),
                (
                    '{"id":7,"ts":"20240701T113349","ev_type":"foo7","user":2}',
                    '{"id":7,"ts":"11:33:49","user_id":2,"lookup":"ydb20"}',
                ),
            ]
            * 10
        ),
    ),
    # 4
    (
        R'''
            $input = SELECT * FROM myyds.`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            id Int32,
                            ts String,
                            ev_type String,
                            user Int32,
                        )
                    )            ;

            $formatTime = DateTime::Format("%H:%M:%S");

            $enriched = select e.id as id,
                            $formatTime(DateTime::ParseIso8601(e.ts)) as ts,
                            e.user as user_id,
                            u.id as uid,
                            u.name as name,
                            u.age as age
                from
                    $input as e
                left join {streamlookup} ydb_conn_{table_name}.`users` as u
                on(e.user = u.id)
            ;

            insert into myyds.`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                (
                    '{"id":1,"ts":"20240701T113344","ev_type":"foo1","user":2}',
                    '{"id":1,"ts":"11:33:44","uid":2,"user_id":2,"name":"Petr","age":25}',
                ),
                (
                    '{"id":2,"ts":"20240701T112233","ev_type":"foo2","user":1}',
                    '{"id":2,"ts":"11:22:33","uid":1,"user_id":1,"name":"Anya","age":15}',
                ),
                (
                    '{"id":3,"ts":"20240701T113355","ev_type":"foo3","user":100}',
                    '{"id":3,"ts":"11:33:55","uid":null,"user_id":100,"name":null,"age":null}',
                ),
                (
                    '{"id":4,"ts":"20240701T113356","ev_type":"foo4","user":3}',
                    '{"id":4,"ts":"11:33:56","uid":3,"user_id":3,"name":"Masha","age":17}',
                ),
                (
                    '{"id":5,"ts":"20240701T113357","ev_type":"foo5","user":3}',
                    '{"id":5,"ts":"11:33:57","uid":3,"user_id":3,"name":"Masha","age":17}',
                ),
                (
                    '{"id":6,"ts":"20240701T112238","ev_type":"foo6","user":1}',
                    '{"id":6,"ts":"11:22:38","uid":1,"user_id":1,"name":"Anya","age":15}',
                ),
                (
                    '{"id":7,"ts":"20240701T113349","ev_type":"foo7","user":2}',
                    '{"id":7,"ts":"11:33:49","uid":2,"user_id":2,"name":"Petr","age":25}',
                ),
            ]
            * 10000
        ),
    ),
    # 5
    (
        R'''
            $input = SELECT * FROM myyds.`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            id Int32,
                            ts String,
                            ev_type String,
                            user Int32,
                        )
                    )            ;

            $enriched = select e.id as id,
                            e.user as user_id,
                            eu.id as uid
                from
                    $input as e
                left join {streamlookup} ydb_conn_{table_name}.`users` as eu
                on(e.user = eu.id)
            ;

            insert into myyds.`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        [
            (
                '{"id":1,"ts":"20240701T113344","ev_type":"foo1","user":2}',
                '{"id":1,"uid":2,"user_id":2}',
            ),
            (
                '{"id":2,"ts":"20240701T112233","ev_type":"foo2","user":1}',
                '{"id":2,"uid":1,"user_id":1}',
            ),
            (
                '{"id":3,"ts":"20240701T113355","ev_type":"foo3","user":100}',
                '{"id":3,"uid":null,"user_id":100}',
            ),
            (
                '{"id":4,"ts":"20240701T113356","ev_type":"foo4","user":3}',
                '{"id":4,"uid":3,"user_id":3}',
            ),
        ],
    ),
    # 6
    (
        R'''
            $input = SELECT * FROM myyds.`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            za Int32,
                            yb STRING,
                            yc Int32,
                            zd Int32,
                        )
                    )            ;

            $enriched = select a, b, c, d, e, f, za, yb, yc, zd
                from
                    $input as e
                left join {streamlookup} ydb_conn_{table_name}.db as u
                on(e.yb = u.b AND e.za = u.a )
            ;

            insert into myyds.`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                (
                    '{"id":1,"za":1,"yb":"2","yc":100,"zd":101}',
                    '{"a":1,"b":"2","c":3,"d":4,"e":5,"f":6,"za":1,"yb":"2","yc":100,"zd":101}',
                ),
                (
                    '{"id":2,"za":7,"yb":"8","yc":106,"zd":107}',
                    '{"a":7,"b":"8","c":9,"d":10,"e":11,"f":12,"za":7,"yb":"8","yc":106,"zd":107}',
                ),
                (
                    '{"id":3,"za":2,"yb":"1","yc":114,"zd":115}',
                    '{"a":null,"b":null,"c":null,"d":null,"e":null,"f":null,"za":2,"yb":"1","yc":114,"zd":115}',
                ),
                (
                    '{"id":3,"za":2,"yb":null,"yc":114,"zd":115}',
                    '{"a":null,"b":null,"c":null,"d":null,"e":null,"f":null,"za":2,"yb":null,"yc":114,"zd":115}',
                ),
            ]
        ),
    ),
    # 7
    (
        R'''
            $input = SELECT * FROM myyds.`{input_topic}`
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            za Int32,
                            yb STRING,
                            yc Int32,
                            zd Int32,
                        )
                    )            ;

            $enriched = select a, b, c, d, e, f, za, yb, yc, zd
                from
                    $input as e
                left join {streamlookup} ydb_conn_{table_name}.db as u
                on(e.za = u.a AND e.yb = u.b)
            ;

            insert into myyds.`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        ResequenceId(
            [
                (
                    '{"id":1,"za":1,"yb":"2","yc":100,"zd":101}',
                    '{"a":1,"b":"2","c":3,"d":4,"e":5,"f":6,"za":1,"yb":"2","yc":100,"zd":101}',
                ),
                (
                    '{"id":2,"za":7,"yb":"8","yc":106,"zd":107}',
                    '{"a":7,"b":"8","c":9,"d":10,"e":11,"f":12,"za":7,"yb":"8","yc":106,"zd":107}',
                ),
                (
                    '{"id":3,"za":2,"yb":"1","yc":114,"zd":115}',
                    '{"a":null,"b":null,"c":null,"d":null,"e":null,"f":null,"za":2,"yb":"1","yc":114,"zd":115}',
                ),
                (
                    '{"id":3,"za":null,"yb":"1","yc":114,"zd":115}',
                    '{"a":null,"b":null,"c":null,"d":null,"e":null,"f":null,"za":null,"yb":"1","yc":114,"zd":115}',
                ),
            ]
        ),
    ),
    # 8
    (
        R'''
            $input = SELECT * FROM myyds.`{input_topic}`
                     WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            time Uint64,
                            key String,
                            message String,
                        )
                    );

            $enriched = SELECT e.`key` as `key`,
                    u.id as uid, e.time as time
                FROM
                    $input AS e
                LEFT JOIN {streamlookup} ydb_conn_{table_name}.`messages` AS u
                ON(e.message = u.msg)
            ;

            insert into myyds.`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        RandomizeMessage(
            RandomizeMessage(
                ResequenceId(
                    [
                        (
                            '{"time":1,"key":"foobar","message":"Message5"}',
                            '{"time":1,"key":"foobar","uid":5}',
                        ),
                    ]
                    * 500000,
                    field='time',
                ),
                field='message',
                key='uid',
                biglen=10000,
            ),
            field='key',
            key='kid',
            biglen=16,
            header='key',
        ),
    ),
    # 9
    (
        R'''
            $input = SELECT * FROM myyds.`{input_topic}`
                     WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            id Uint64,
                            age Uint32,
                            key String,
                        )
                    );

            $enriched = SELECT u.name as name, key,
                    e.id as id, e.age as age
                FROM
                    $input AS e
                LEFT JOIN {streamlookup} ydb_conn_{table_name}.`dbx` AS u
                ON(e.id = u.id AND e.age = u.age)
                -- ON(e.age = u.age AND e.id = u.id)
            ;

            insert into myyds.`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $enriched;
            ''',
        RandomizeDBX(
            [
                (
                    '{"id":1,"age":1,"key":"Message5"}',
                    '{"name":null,"id":123,"age":456,"key":0}',
                ),
            ]
            * 7000,
        ),
    ),
    # 10
    (
        R'''
            $input = SELECT * FROM myyds.`{input_topic}`
                     WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            id Uint64,
                            age Uint32,
                            hash String,
                            key String,
                        )
                    );

            $enriched = SELECT e.hash as hash, key,
                    u.id as uid, u.age as uage
                FROM
                    $input AS e
                LEFT JOIN {streamlookup} ydb_conn_{table_name}.`dby` AS u
                ON(e.hash = u.hash)
            ;
            $formatTime = DateTime::Format("%Y%m%d%H%M%S");
            $preout = SELECT hash, key, uid, uage, $formatTime(CurrentUtcTimestamp(key)) as utc FROM $enriched;

            insert into myyds.`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $preout;
            ''',
        RandomizeDBH(
            [
                (
                    '{"id":1,"age":1,"hash":null,"key":"Message5"}',
                    '{"hash":null,"uid":123,"uage":456,"key":0}',
                ),
            ]
            * 1000000,
        ),
    ),
    # 11
    (
        R'''
            $input = SELECT * FROM myyds.`{input_topic}`
                     WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            id Uint64,
                            age Uint32,
                            hash String,
                            key String,
                        )
                    );

            $enriched = SELECT e.hash as hash, key,
                    u.id as uid, u.age as uage
                FROM
                    $input AS e
                LEFT JOIN {streamlookup} ydb_conn_{table_name}.`dbz` AS u
                ON(String::HexDecode(e.hash) = u.hash)
            ;
            $formatTime = DateTime::Format("%Y%m%d%H%M%S");
            $preout = SELECT hash, key, uid, uage, $formatTime(CurrentUtcTimestamp(key)) as utc FROM $enriched;

            insert into myyds.`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $preout;
            ''',
        RandomizeDBH(
            [
                (
                    '{"id":1,"age":1,"hash":null,"key":"Message5"}',
                    '{"hash":null,"uid":123,"uage":456,"key":0}',
                ),
            ]
            * 1000000,
        ),
    ),
    # 12
    (
        R'''
            $input = SELECT * FROM myyds.`{input_topic}`
                     WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            ev String,
                        )
                    );

            $i = select e.ev as ev, String::ToByteList(Coalesce(String::Base64Decode(e.ev),'')) as bl from $input as e;

            $t = select e.ev as ev, db.`dict` as `dict`, e.bl as bl
                from $i as e
                left join /*+ streamlookup(TTL 109 MaxDelayedRows 100 MaxCachedRows 12450) */ ydb_conn_{table_name}.big as db
                on (CAST(e.bl[0] AS Uint64) = db.prefix)
            ;
            $enriched =
            select ev, String::Substring(e.`dict`,(cast(e.bl[2] as Uint32)*65536u+cast(e.bl[1] as Uint32)*256u+cast(e.bl[0] as Uint32))%(Length(e.`dict`)-4),4) as xx from $t as e;

            $formatTime = DateTime::Format("%Y%m%d%H%M%S");
            $preout = SELECT ev, xx, $formatTime(CurrentUtcTimestamp(ev)) as utc FROM $enriched;

            insert into myyds.`{output_topic}`
            select Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) from $preout;
            ''',
        RandomizeBIG(
            [
                (
                    '{"ev":"abc"}',
                    '{"ev":"abc","xx":"yyy"}',
                ),
            ]
            * 100000,
        ),
    ),
]

if not XD:
    # TESTCASES = TESTCASES[1:2]
    TESTCASES = TESTCASES[12:13]
    pass


one_time_waiter = OneTimeWaiter(
    data_source_kind=EDataSourceKind.YDB,
    docker_compose_file_path=conftest.docker_compose_file_path,
    expected_tables=["simple_table", "join_table", "dummy_table"],
)


class TestJoinStreaming(TestYdsBase):
    def restart_node(self, kikimr, query_id):
        # restart node with CA

        node_to_restart = None

        for node_index in kikimr.compute_plane.kikimr_cluster.nodes:
            wc = kikimr.compute_plane.get_worker_count(node_index)
            if wc is not None:
                if wc > 0 and node_to_restart is None:
                    node_to_restart = node_index
        assert node_to_restart is not None, "Can't find any task on node"

        logging.debug("Restart compute node {}".format(node_to_restart))

        kikimr.compute_plane.kikimr_cluster.nodes[node_to_restart].stop()
        kikimr.compute_plane.kikimr_cluster.nodes[node_to_restart].start()
        kikimr.compute_plane.wait_bootstrap(node_to_restart)

    @yq_v1
    @pytest.mark.parametrize(
        "mvp_external_ydb_endpoint", [{"endpoint": "tests-fq-generic-streaming-ydb:2136"}], indirect=True
    )
    @pytest.mark.parametrize("fq_client", [{"folder_id": "my_folder"}], indirect=True)
    def test_simple(self, kikimr, fq_client: FederatedQueryClient, yq_version):
        self.init_topics(f"pq_yq_streaming_test_simple{yq_version}")
        fq_client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))

        table_name = 'join_table'
        ydb_conn_name = f'ydb_conn_{table_name}'

        fq_client.create_ydb_connection(
            name=ydb_conn_name,
            database_id='local',
        )
        one_time_waiter.wait()

        sql = R'''
            $input = SELECT * FROM myyds.`{input_topic}`;

            $enriched = select e.Data as Data
                from
                    $input as e
                left join
                    ydb_conn_{table_name}.{table_name} as u
                on(e.Data = CAST(u.id as String))
            ;

            insert into myyds.`{output_topic}`
            select * from $enriched;
            '''.format(
            input_topic=self.input_topic, output_topic=self.output_topic, table_name=table_name
        )

        query_id = fq_client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        fq_client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        messages = ['A', 'B', 'C']
        self.write_stream(messages)

        read_data = self.read_stream(len(messages))
        assert read_data == messages

        fq_client.abort_query(query_id)
        fq_client.wait_query(query_id)

        describe_response = fq_client.describe_query(query_id)
        status = describe_response.result.query.meta.status
        assert not describe_response.issues, str(describe_response.issues)
        assert status == fq.QueryMeta.ABORTED_BY_USER, fq.QueryMeta.ComputeStatus.Name(status)

    @yq_v1
    @pytest.mark.parametrize(
        "mvp_external_ydb_endpoint", [{"endpoint": "tests-fq-generic-streaming-ydb:2136"}], indirect=True
    )
    @pytest.mark.parametrize("fq_client", [{"folder_id": "my_folder_slj"}], indirect=True)
    @pytest.mark.parametrize("partitions_count", [1, 11] if DEBUG and XD else [11])
    @pytest.mark.parametrize("streamlookup", [False, True] if DEBUG and XD else [True])
    @pytest.mark.parametrize("testcase", [*range(len(TESTCASES))])
    @pytest.mark.parametrize("test_checkpoints", [False])
    def test_streamlookup(
        self,
        kikimr,
        test_checkpoints,
        testcase,
        streamlookup,
        partitions_count,
        fq_client: FederatedQueryClient,
        yq_version,
    ):
        if test_checkpoints and not WITH_CHECKPOINTS:
            return
        self.init_topics(
            f"pq_yq_str_lookup_{partitions_count}{streamlookup}{testcase}_{yq_version}",
            partitions_count=partitions_count,
        )
        fq_client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))

        table_name = 'join_table'
        ydb_conn_name = f'ydb_conn_{table_name}'

        fq_client.create_ydb_connection(
            name=ydb_conn_name,
            database_id='local',
        )

        sql, messages = TESTCASES[testcase]
        sql = sql.format(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
            table_name=table_name,
            streamlookup=R'/*+ streamlookup() */' if streamlookup else '',
        )
        if not WITH_CHECKPOINTS:
            sql = 'PRAGMA dq.DisableCheckpoints="true";\n' + sql

        one_time_waiter.wait()

        query_id = fq_client.create_query(
            f"streamlookup_{partitions_count}{streamlookup}{testcase}", sql, type=fq.QueryContent.QueryType.STREAMING
        ).result.query_id
        fq_client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

        if WITH_CHECKPOINTS:
            kikimr.compute_plane.wait_zero_checkpoint(query_id)
        else:
            kikimr.control_plane.wait_worker_count(1, "DQ_PQ_READ_ACTOR", 1)

        if test_checkpoints:
            last_row = 0
            last_checkpoint = kikimr.compute_plane.get_completed_checkpoints(query_id)

        offset = 0
        while offset < len(messages):
            chunk = messages[offset : offset + 500]
            self.write_stream(map(lambda x: x[0], chunk))
            offset += 500
            time.sleep(0.001)
            if test_checkpoints:
                if offset >= last_row + 5000:
                    current_checkpoint = kikimr.compute_plane.get_completed_checkpoints(query_id)
                    if current_checkpoint >= last_checkpoint + 2:
                        self.restart_node(kikimr, query_id)
                        last_checkpoint = current_checkpoint
                    last_row = offset

        print(messages, file=sys.stderr, sep="\n")

        read_data = self.read_stream(len(messages))

        if DEBUG or 1:
            print(streamlookup, testcase, file=sys.stderr)
            print(sql, file=sys.stderr)
            print(*zip(messages, read_data), file=sys.stderr, sep="\n")

        def rmutc(d):
            if 'utc' in d:
                del d['utc']
            return d

        read_data_ctr = Counter(map(freeze, map(rmutc, map(json.loads, read_data))))
        messages_ctr = Counter(map(freeze, map(json.loads, map(itemgetter(1), messages))))

        if False:
            assert read_data_ctr == messages_ctr
        else:
            assert len(read_data_ctr) == len(messages_ctr)
            ctr = 0
            for k in read_data_ctr:
                assert read_data_ctr[k] == messages_ctr[k], f'mismatch at {k}: {read_data_ctr[k]} != {messages_ctr[k]}'
                ctr += 1
                if ctr == 1000:
                    print('<#>', file=sys.stderr, flush=True, end='')
                    ctr = 0

        for node_index in kikimr.compute_plane.kikimr_cluster.nodes:
            sensors = kikimr.compute_plane.get_sensors(node_index, "dq_tasks")
            for component in ["Lookup", "LookupSrc"]:
                componentSensors = sensors.find_sensors(
                    labels={"operation": query_id, "component": component},
                    key_label="sensor",
                )
                for k in componentSensors:
                    print(
                        f'node[{node_index}].operation[{query_id}].component[{component}].{k} = {componentSensors[k]}',
                        file=sys.stderr,
                    )

        fq_client.abort_query(query_id)
        fq_client.wait_query(query_id)

        describe_response = fq_client.describe_query(query_id)
        status = describe_response.result.query.meta.status
        assert not describe_response.issues, str(describe_response.issues)
        assert status == fq.QueryMeta.ABORTED_BY_USER, fq.QueryMeta.ComputeStatus.Name(status)
