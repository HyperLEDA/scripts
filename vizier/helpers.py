def sanitize_filename(string: str) -> str:
    return string.replace("/", "_")


def get_filename(catalog_name: str, table_name: str) -> str:
    return f"{sanitize_filename(catalog_name)}_{sanitize_filename(table_name)}"
