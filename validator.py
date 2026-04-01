def validate(data):
    errors = []
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
            errors.append(field)
            continue

        if isinstance(value, str) and value.strip() == "":
            errors.append(field)
            continue
    return errors
