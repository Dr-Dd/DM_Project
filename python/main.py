#!/usr/bin/env python3
import io
import os
import psycopg
import csv
import rapidgzip
import contextlib

from Constants import conn_string
from Registry import copy_templates
from datetime import datetime

def handle_files():
    for o in copy_templates:
        print(f"{datetime.now()} Working on {o.filename}", flush=True)
        with rapidgzip.open(os.path.join("/data", o.filename), parallelization=os.cpu_count()) as d:
            with io.TextIOWrapper(d, encoding='utf-8') as text_stream:
                r = csv.DictReader(text_stream, delimiter="\t", quoting=csv.QUOTE_NONE)
                with contextlib.ExitStack() as stack:
                # Create connections, cursors, copy objs and contexts
                    for k, v in o.get_schema().items():
                        cn = psycopg.connect(conn_string)
                        cur = cn.cursor()
                        cur.execute(f"SELECT EXISTS (SELECT 1 FROM {k} LIMIT 1);")
                        if cur.fetchone()[0]:
                            print(f"{datetime.now()} Database already populated, quitting", flush=True)
                            exit(1)
                        cur.execute(f"ALTER TABLE {k} DISABLE TRIGGER ALL;")
                        cp = cur.copy(o.generate_copy_statement(k,v))
                        ctx = stack.enter_context(cp)
                        o.conn_cur_cp_ctx_dict[k] = (cn, cur, cp, ctx)
                    # Hot loop, start reading line by line
                    for l in r:
                        try:
                            o.ingest_row(l)
                        except Exception as e:
                            print(l, flush=True)
                            for t, (cn, cur, _, _) in o.conn_cur_cp_ctx_dict.items():
                                cn.rollback()
                                cur.execute(f"ALTER TABLE {t} ENABLE TRIGGER ALL;")
                                cn.commit()
                                cn.close()
                            raise
                for t, (cn, cur, _, _) in o.conn_cur_cp_ctx_dict.items():
                    cn.commit()
                    cur.execute(f"ALTER TABLE {t} ENABLE TRIGGER ALL;")
                    cn.commit()
                    cn.close()

def main():
    print("Starting etl process...", flush=True)
    handle_files()
    print("Etl process completed, exiting.", flush=True)
    exit(0)

if __name__ == "__main__":
    main()
