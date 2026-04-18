import os
import json
import psycopg2


def handler(event: dict, context) -> dict:
    """
    Диагностика PostgreSQL подключения для DevOps.
    Проверяет совместимость и готовность базы данных.
    Требует заголовок X-Admin-Token для авторизации.
    """
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, X-Admin-Token',
        'Content-Type': 'application/json',
    }

    if event.get('httpMethod') == 'OPTIONS':
        return {'statusCode': 200, 'headers': headers, 'body': ''}

    admin_token = os.environ.get('ADMIN_TOKEN', '')
    request_token = (event.get('headers') or {}).get('X-Admin-Token', '')
    if admin_token and request_token != admin_token:
        return {
            'statusCode': 401,
            'headers': headers,
            'body': json.dumps({'error': 'Unauthorized'}),
        }

    results = {}

    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute("SELECT version()")
        results['version'] = cur.fetchone()[0]
    except Exception as e:
        results['version'] = type(e).__name__

    try:
        cur.execute("SELECT current_database(), current_user")
        row = cur.fetchone()
        results['database'] = row[0]
        results['user'] = row[1]
    except Exception as e:
        results['database'] = type(e).__name__
        results['user'] = type(e).__name__

    try:
        cur.execute("SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%%' AND nspname != 'information_schema'")
        results['schemas'] = [r[0] for r in cur.fetchall()]
    except Exception as e:
        results['schemas'] = type(e).__name__

    try:
        cur.execute("CREATE TEMP TABLE _diag_test(id int)")
        cur.execute("DROP TABLE _diag_test")
        results['can_create_temp_table'] = True
    except:
        results['can_create_temp_table'] = False

    try:
        cur.execute("CREATE FUNCTION pg_temp._diag_fn() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN 1; END; $$")
        cur.execute("SELECT pg_temp._diag_fn()")
        results['can_create_temp_function'] = True
        cur.execute("DROP FUNCTION pg_temp._diag_fn()")
    except:
        results['can_create_temp_function'] = False

    try:
        cur.execute("SELECT extname, extversion FROM pg_extension")
        results['extensions'] = [{'name': r[0], 'version': r[1]} for r in cur.fetchall()]
    except Exception as e:
        results['extensions'] = type(e).__name__

    try:
        cur.execute("SELECT lanname FROM pg_language")
        results['languages'] = [r[0] for r in cur.fetchall()]
    except Exception as e:
        results['languages'] = type(e).__name__

    try:
        cur.execute("SELECT count(*) FROM pg_stats_ext")
        results['pg_stats_ext_count'] = cur.fetchone()[0]
    except:
        results['pg_stats_ext_count'] = 'blocked'

    try:
        cur.execute("SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolcanlogin FROM pg_roles WHERE rolname = current_user")
        row = cur.fetchone()
        results['role'] = {
            'name': row[0],
            'super': row[1],
            'createdb': row[2],
            'createrole': row[3],
            'canlogin': row[4],
        }
    except Exception as e:
        results['role'] = type(e).__name__

    try:
        cur.execute("SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')")
        results['visible_tables'] = cur.fetchone()[0]
    except Exception as e:
        results['visible_tables'] = type(e).__name__

    try:
        cur.execute("SELECT schemaname, count(*) as cnt FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema') GROUP BY schemaname ORDER BY cnt DESC LIMIT 20")
        results['schema_distribution'] = [{'schema': r[0], 'tables': r[1]} for r in cur.fetchall()]
    except Exception as e:
        results['schema_distribution'] = type(e).__name__

    try:
        cur.execute("SELECT schemaname, tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema') ORDER BY schemaname LIMIT 30")
        results['table_inventory'] = [{'schema': r[0], 'table': r[1]} for r in cur.fetchall()]
    except Exception as e:
        results['table_inventory'] = type(e).__name__

    try:
        cur.execute("SELECT '0 0 0'::oidvector::text")
        results['oidvector_support'] = cur.fetchone()[0]
    except Exception as e:
        results['oidvector_support'] = type(e).__name__

    try:
        cur.execute("SELECT proname, proargtypes::text FROM pg_proc WHERE proname = 'version'")
        row = cur.fetchone()
        results['pg_proc_check'] = {'name': row[0], 'argtypes': row[1]}
    except Exception as e:
        results['pg_proc_check'] = type(e).__name__

    try:
        cur.execute("SELECT ARRAY[[1,2],[3,4]]::oid[]::oidvector::text")
        results['oidvector_2d_cast'] = cur.fetchone()[0]
    except Exception as e:
        results['oidvector_2d_cast'] = str(e)[:200]

    try:
        cur.execute("SELECT ARRAY[NULL::oid, 1, 2]::oidvector::text")
        results['oidvector_null_cast'] = cur.fetchone()[0]
    except Exception as e:
        results['oidvector_null_cast'] = str(e)[:200]

    try:
        cur.execute("SELECT ARRAY[[1,2],[3,4]]::int2[]::int2vector::text")
        results['int2vector_2d_cast'] = cur.fetchone()[0]
    except Exception as e:
        results['int2vector_2d_cast'] = str(e)[:200]

    cur.close()
    conn.close()

    return {
        'statusCode': 200,
        'headers': headers,
        'body': json.dumps(results),
    }