import pymysql



def call_import_date_dim_procedure(tenTienTrinh, mota, status, job):
    conn = pymysql.connect(
        host=job['config']["ip"] or "localhost",
        port=job['config']["port"],
        user=job['config']["user"],
        password=job['config']["password"],
        db=job['config']["database"]
    )
    try:           
        with conn.cursor() as cur:

            sql = """
                INSERT INTO process_log (name, description, status, created_at, updated_at, job_id)
                VALUES (%s, %s, %s, NOW(), NOW(), %s)
            """
            cur.execute(sql, (tenTienTrinh, mota, status, job['jobConfig']['id']))
            conn.commit()
            cur.execute("SELECT LAST_INSERT_ID() AS id") 
            result = cur.fetchone() 
            process_id = result[0] 
            return process_id 
    finally:
        conn.close()


def update_status_by_id(job, process_id, new_status):
    conn = pymysql.connect(
        host=job['config']["ip"] or "localhost",
        port=job['config']["port"],
        user=job['config']["user"],
        password=job['config']["password"],
        db=job['config']["database"]
    )
    
    try:
        with conn.cursor() as cur:
            sql = """
                UPDATE process_log
                SET status = %s,
                    updated_at = NOW()
                WHERE id = %s
            """
            cur.execute(sql, (new_status, process_id))
            
            conn.commit()
            print(f" Đã cập nhật status = '{new_status}' cho id = {process_id}")
    finally:
        conn.close()
        
def get_latest_today_process_log(job):
    conn = pymysql.connect(
        host=job['config']["ip"] or "localhost",
        port=job['config']["port"],
        user=job['config']["user"],
        password=job['config']["password"],
        db=job['config']["database"]
    )
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:

            sql = """
                SELECT *
                FROM process_log AS pl
                WHERE DAY(pl.created_at) = DAY(CURRENT_DATE)
                  AND MONTH(pl.created_at) = MONTH(CURRENT_DATE)
                  AND YEAR(pl.created_at) = YEAR(CURRENT_DATE)
                ORDER BY pl.created_at DESC
                LIMIT 1;
            """
            cur.execute(sql)
            return cur.fetchone()  
    finally:
        conn.close()        
