import logging

DB_COLUMNS = [
    "FILE_NAME", 
    "AGENCY_CODE",
    "AGENCY_NAME",
    "CLIENT_CODE",
    "CLIENT_NAME",
    "RO_CLIENT_CODE",
    "RO_CLIENT_NAME", 
    "RO_NUMBER",
    "RO_DATE",
    "KEY_NUMBER",
    "EXECUTIVE_NAME",
    "EXECUTIVE_CODE",
    "COLOUR",
    "AD_CAT",
    "AD_SUBCAT",
    "PRODUCT",
    "BRAND",
    "PACKAGE_NAME",
    "INSERT_DATE",
    "AD_HEIGHT",
    "AD_WIDTH",
    "AD_SIZE",
    "PAGE_PREMIUM",
    "RO_AMOUNT",
    "RO_RATE",
    "BOOKING_CENTER",
    "RO_REMARKS",
    "EXTRACTED_TEXT",
    "POSITIONING",
    "AD_DISCOUNT"
]

MAX_BIND_LENGTHS = {
    "AD_SIZE": 50,
}

SQL = """
INSERT INTO RO_OCR_DATA (
    FILE_NAME,
    AGENCY_CODE,
    AGENCY_NAME,
    CLIENT_CODE,
    CLIENT_NAME,
    RO_CLIENT_CODE,
    RO_CLIENT_NAME,
    RO_NUMBER,
    RO_DATE,
    KEY_NUMBER,
    EXECUTIVE_NAME,
    EXECUTIVE_CODE,
    COLOUR,
    AD_CAT,
    AD_SUBCAT,
    PRODUCT,
    BRAND,
    PACKAGE_NAME,
    INSERT_DATE,
    AD_HEIGHT,
    AD_WIDTH,
    AD_SIZE,
    PAGE_PREMIUM,
    RO_AMOUNT,
    RO_RATE,
    BOOKING_CENTER,
    RO_REMARKS,
    EXTRACTED_TEXT,
    POSITIONING,
    AD_DISCOUNT,
    CREATED_DT
) VALUES (
    :FILE_NAME,
    :AGENCY_CODE,
    :AGENCY_NAME,
    :CLIENT_CODE,
    :CLIENT_NAME,
    :RO_CLIENT_CODE,
    :RO_CLIENT_NAME,
    :RO_NUMBER,
    TO_DATE(:RO_DATE, 'DD-MM-YYYY'),
    :KEY_NUMBER,
    :EXECUTIVE_NAME,
    :EXECUTIVE_CODE,
    :COLOUR,
    :AD_CAT,
    :AD_SUBCAT,
    :PRODUCT,
    :BRAND,
    :PACKAGE_NAME,
    TO_DATE(:INSERT_DATE, 'DD-MM-YYYY'),
    :AD_HEIGHT,
    :AD_WIDTH,
    :AD_SIZE,
    :PAGE_PREMIUM,
    :RO_AMOUNT,
    :RO_RATE,
    :BOOKING_CENTER,
    :RO_REMARKS,
    :EXTRACTED_TEXT,
    :POSITIONING,
    :AD_DISCOUNT,
    SYSDATE
)
"""


def insert_ro_data(conn, data: dict):
    bind_data = {}
    for col in DB_COLUMNS:
        value = data.get(col)
        if value == "":
            value = None
        # Truncate RO_REMARKS to 1000 chars BEFORE adding to bind_data
        if col == "RO_REMARKS" and value and len(value) > 1000:
            value = value[:1000]
            logging.warning("RO_REMARKS truncated from %d to 1000 chars", len(data.get(col)))
        if col in MAX_BIND_LENGTHS and isinstance(value, str) and len(value) > MAX_BIND_LENGTHS[col]:
            logging.warning(
                "%s truncated from %d to %d chars",
                col,
                len(value),
                MAX_BIND_LENGTHS[col],
            )
            value = value[:MAX_BIND_LENGTHS[col]]
        bind_data[col] = value
    
    logging.debug("FINAL DB BINDS:")
    for k, v in bind_data.items():
        logging.debug("  %s = %s", k, v)
    
    cur = conn.cursor()
    cur.execute(SQL, bind_data)
    conn.commit()
    logging.info("DB INSERT SUCCESS")
