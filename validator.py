def validate_mandatory_fields(data):
    missing = []
    for field in [
        "AGENCY_NAME",
        "CLIENT_NAME",
        "CLIENT_CODE",
        "RO_NUMBER",
        "RO_DATE",
        "INSERT_DATE"
    ]:
        value = data.get(field)
        if value is None:
            missing.append(field)
            continue

        if isinstance(value, str) and value.strip() == "":
            missing.append(field)
            continue
    return missing