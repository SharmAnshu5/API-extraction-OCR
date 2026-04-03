from identifier import identify
from parser_api import parse_pdf, parse_image


def route(file_path):
    t = identify(file_path)
    if t == "PDF":
        return parse_pdf(file_path)
    elif t == "IMAGE":
        return parse_image(file_path)
    else:
        raise ValueError("Unsupported file")