import os
import oracledb
import logging
import dotenv

DB_USER = os.getenv("DB_USER", "auerp")
DB_PASSWORD = os.getenv("DB_PASSWORD", "auerp")
DB_DSN = os.getenv("DB_DSN", "ERPDB")
DB_CLIENT_LIB = os.getenv("DB_CLIENT_LIB", "C:/instantclient_23_9")

dotenv.load_dotenv()
logger = logging.getLogger("db_connection")

try:
    if hasattr(oracledb, "DB_MODE_THICK"):
        oracledb.init_oracle_client(lib_dir=DB_CLIENT_LIB)
        oracledb.defaults.driver_mode = oracledb.DB_MODE_THICK
        logger.info("Oracle client initialized in THICK mode")
    else:
        oracledb.init_oracle_client(lib_dir=DB_CLIENT_LIB)
        logger.info("Oracle client initialized")
except Exception as e:
    logger.warning(f"Oracle client init skipped or failed: {e}")

def db_connect():
    try:
        conn = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN)
        logger.info("Connected to Oracle database")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None


def ensure_connection(conn):
    try:
        if conn is None:
            raise Exception("Connection object is None")
        conn.ping()
        return conn
    except Exception as e:
        logger.warning(f"Oracle connection lost — reconnecting... ({e})")
        return db_connect()
    
def fetch_pending_rows(conn):
    try:
        cur = conn.cursor()
        cur.execute(f"""SELECT * FROM RO_OCR_DATA WHERE TRUNC(CREATED_DT ) = TRUNC(SYSDATE) ORDER BY CREATED_DT DESC""" )       
        columns = [col[0] for col in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        cur.close()
        logger.info(f"Fetched {len(rows)} pending rows as dicts")
        return rows
    except Exception as e:
        logger.error(f"fetch_pending_rows failed: {e}")
        return []
    
def fetch_pending_EMAIL(adbook, conn):
    try:
        unit = adbook.get("BOOKING_CENTER")
        cur = conn.cursor()
        cur.execute("""
            SELECT EMAILFROM, EMAILTO, EMAILCC, UNIT_CODE, MAILTYPEFORM
            FROM SL_ADVICE_EMAIL
            WHERE MAILTYPEFORM = 'H' AND UNIT_CODE = :1
        """, [unit])
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        columns = ["EMAILFROM", "EMAILTO", "EMAILCC"]
        return dict(zip(columns, row))
    except Exception as e:
        logger.error(f"fetch_pending_EMAIL failed: {e}")
        return None
    
def fetch_pending_EMAIL_Issue(adbook, conn):
    try:
        unit = adbook.get("BOOKING_CENTER")
        cur = conn.cursor()
        cur.execute("""
            SELECT EMAILFROM, EMAILTO, EMAILCC, UNIT_CODE, MAILTYPEFORM
            FROM SL_ADVICE_EMAIL
            WHERE MAILTYPEFORM = 'M' AND UNIT_CODE = :1
        """, [unit])
        row = cur.fetchone()
        cur.close()
        if not row:
            return None 
        columns = ["EMAILFROM", "EMAILTO", "EMAILCC"]
        return dict(zip(columns, row))
    except Exception as e:
        logger.error(f"fetch_pending_EMAIL failed: {e}")
        return None    
    
def fetch_Card_Rate(conn, booking_id):
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                "Agrred_rate",
                "Agreed_amount",
                "Card_rate",
                "Discount",
                "Discount_per",
                "Contract_rate",
                "Gross_amount"
            FROM TBL_BOOKING_MAST
            WHERE "cio_booking_id" = :1
        """, (str(booking_id),))
        row = cur.fetchone()
        if not row:
            logger.warning(f"No card rate found for booking_id={booking_id}")
            return {}
        columns = [col[0] for col in cur.description]
        data = dict(zip(columns, row))
        cur.close()
        logger.info(f"Card rate fetched for booking_id={booking_id}")
        return data
    except Exception as e:
        logger.error(f"fetch_Card_Rate failed: {e}", exc_info=True)
        return {}    

    