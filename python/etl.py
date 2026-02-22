#!/usr/bin/env python3
# Data configuration file
import os
import psycopg
import csv
import rapidgzip
import contextlib

from constants import conn_string, copy_templates


def handle_files():
    for o in copy_templates:
        with rapidgzip.open(os.path.join("/data", o.filename), "rt", parallelization=os.cpu_count()) as d:
            r = csv.DictReader(d, delimiter="\t", quoting=csv.QUOTE_NONE)
            with contextlib.ExitStack() as stack:
            # Create connections, cursors, copy objs and contexts
                for k, v in o.get_schema.items():
                    cn = psycopg.connect(conn_string)
                    cur = cn.cursor()
                    cur.execute(f"ALTER TABLE {k} DISABLE TRIGGER ALL;")
                    cp = cur.copy(o.generate_copy_statement(k,v))
                    ctx = stack.enter_context(cp)
                    o.conn_cur_cp_ctx_dict[k] = (cn, cur, cp, ctx)
                # Hot loop, start reading line by line
                for l in r:
                    o.ingest_row(l)
            for t, (cn, cur, _, _) in o.conn_cur_cp_ctx_dict.items():
                cn.commit()
                cur.execute(f"ALTER TABLE {t} ENABLE TRIGGER ALL;")
                cn.close()





print("Starting etl process...", flush=True)
handle_files()
print("Etl process completed, exiting.", flush=True)
exit(0)
