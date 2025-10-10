# Copyright 2024 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import peewee
import tidb_vector
import tabulate
import h5py
import numpy
import time

from tidb_vector.peewee import VectorField, VectorAdaptor
from tidb_vector.utils import encode_vector

dataset_path = "./datasets/fashion-mnist-784-euclidean.hdf5"
table_name = "recall_test"

mysql_db = peewee.MySQLDatabase(
    "test",
    host="127.0.0.1",
    port=4000,
    user="root",
    passwd="",
)

class Sample(peewee.Model):
    class Meta:
        database = mysql_db
        db_table = table_name

    id = peewee.IntegerField(
        primary_key=True,
    )
    vec = VectorField(784)

def connect():
    print(
        f"+ Connecting to {mysql_db.connect_params['user']}@{mysql_db.connect_params['host']}..."
    )
    mysql_db.connect()

def clean():
    mysql_db.drop_tables([Sample], safe=True)

def load():
    mysql_db.create_tables([Sample])
    VectorAdaptor(mysql_db).create_vector_index(
        Sample.vec, tidb_vector.DistanceMetric.L2
    )

    print()
    print("+ Loading data...")

    with h5py.File(dataset_path, "r") as data_file:
        data: numpy.ndarray = data_file["train"][()]
        data_with_id = [(idx, data[idx]) for idx in range(0, len(data))]
        max_data_id = data_with_id[-1][0]

        for batch in peewee.chunked(data_with_id, 1000):
            print(f"  - Batch insert [{batch[0][0]}..{batch[-1][0]}] (max PK={max_data_id})...")
            Sample.insert_many(batch, fields=[Sample.id, Sample.vec]).execute()

def check(check_tiflash_used_index: bool):
    recall = 0.0

    print()
    print("+ Current index distribution:")
    cursor = mysql_db.execute_sql(
        f"SELECT ROWS_STABLE_INDEXED, ROWS_STABLE_NOT_INDEXED, ROWS_DELTA_INDEXED, ROWS_DELTA_NOT_INDEXED FROM INFORMATION_SCHEMA.TIFLASH_INDEXES WHERE TIDB_TABLE='{table_name}'"
    )
    print(
        tabulate.tabulate(
            cursor.fetchall(),
            headers=[
                "StableIndexed",
                "StableNotIndexed",
                "DeltaIndexed",
                "DeltaNotIndexed",
            ],
            tablefmt="psql",
        )
    )

    with h5py.File(dataset_path, "r") as data_file:
        query_rows = data_file["test"][()]
        query_rows_len = min(len(query_rows), 100)  # Just check with first 100 test rows

        print("+ Execution Plan:")

        with mysql_db.execute_sql(
            f"EXPLAIN ANALYZE SELECT id FROM {table_name} ORDER BY VEC_L2_Distance(vec, %s) LIMIT 100",
            (encode_vector(query_rows[0]),),
        ) as cursor:
            plan = tabulate.tabulate(cursor.fetchall(), tablefmt="psql")
            print(plan)
            assert "mpp[tiflash]" in plan
            assert "annIndex:L2(vec.." in plan
            if check_tiflash_used_index:
                assert "vector_idx:{" in plan

        print()
        print(f"+ Checking recall (via {query_rows_len} groundtruths)...")

        total_recall = 0.0
        total_tests = 0

        for test_rowid in range(query_rows_len):
            query_row: numpy.ndarray = query_rows[test_rowid]
            groundtruth_results_set = set(data_file["neighbors"][test_rowid])

            with mysql_db.execute_sql(
                f"SELECT id FROM {table_name} ORDER BY VEC_L2_Distance(vec, %s) LIMIT 100",
                (encode_vector(query_row),),
            ) as cursor:
                actual_results = cursor.fetchall()
                actual_results_set = set([int(row[0]) for row in actual_results])
                recall = (
                    len(groundtruth_results_set & actual_results_set)
                    / len(groundtruth_results_set)
                    * 100
                )
                total_recall += recall
                total_tests += 1

                if recall < 80:
                    print(
                        f"  - WARNING: groundtruth #{test_rowid} recall {recall:.2f}%"
                    )

        avg_recall = total_recall / total_tests
        print(f"  - Average recall: {recall:.2f}%")

        # For this dataset, our recall is very high, so we set a very high standard here
        assert avg_recall >= 95


def compact():
    print()
    print("+ Compact table...")
    mysql_db.execute_sql(f"ALTER TABLE {table_name} COMPACT")
    print("+ Waiting index build finish...")

    start_time = time.time()
    while True:
        cursor = mysql_db.execute_sql(
            f"SELECT ROWS_STABLE_NOT_INDEXED, ROWS_STABLE_INDEXED FROM INFORMATION_SCHEMA.TIFLASH_INDEXES WHERE TIDB_TABLE='{table_name}'"
        )
        row = cursor.fetchone()
        if row[0] == 0:
            break

        print(f"  - StableIndexed: {row[1]}, StableNotIndexed: {row[0]}")

        time.sleep(30)

        if time.time() - start_time > 600:
            raise Exception("Index build not finished in 10 minutes")


def main():
    connect()
    clean()
    load()
    print("+ Wait 10s so that some delta index may be built...")
    time.sleep(10)
    check(check_tiflash_used_index=False)
    compact()
    check(check_tiflash_used_index=True)
    clean()

if __name__ == "__main__":
    main()