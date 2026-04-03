import logging

logger = logging.getLogger("db_validation")


def ro_exists_in_db(conn, ro_no):
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT COUNT(1)
            FROM RO_OCR_DATA
            WHERE RO_NUMBER = :1
        """, (ro_no,))

        count = cur.fetchone()[0]
        cur.close()

        return count > 0

    except Exception as e:
        logger.error(f"Duplicate check failed: {e}", exc_info=True)
        return False
