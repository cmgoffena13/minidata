def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'
