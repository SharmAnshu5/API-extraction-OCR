def identify(file_path):
    if file_path.lower().endswith(".pdf"):
        return "PDF"
    elif file_path.lower().endswith((".jpg", ".jpeg", ".png")):
        return "IMAGE"
    return "UNKNOWN"