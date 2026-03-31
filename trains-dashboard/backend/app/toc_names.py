"""Static mapping of TRUST numeric TOC codes to operator names.

TRUST codes are assigned by Network Rail and change infrequently.
The dict is intentionally best-effort — unknown codes fall back to
"TOC <code>" in the helper function.
"""

# Source: Network Rail open data documentation / industry knowledge.
TOC_NAMES: dict[str, str] = {
    "11": "Southern",
    "15": "Thameslink",
    "20": "TransPennine Express",
    "23": "Transport for Wales",
    "24": "Great Northern",
    "27": "Avanti West Coast",
    "28": "East Midlands Railway",
    "32": "London Overground",
    "41": "Chiltern Railways",
    "51": "Hull Trains",
    "52": "Grand Central",
    "53": "Heathrow Express",
    "61": "South Western Railway",
    "70": "c2c",
    "71": "Great Western Railway",
    "73": "Southeastern",
    "74": "Thameslink",
    "75": "Chiltern Railways",
    "76": "CrossCountry",
    "79": "Island Line",
    "80": "Merseyrail",
    "84": "Northern",
    "86": "Greater Anglia",
    "88": "East Midlands Railway",
    "94": "LNER",
    "95": "ScotRail",
    "96": "Caledonian Sleeper",
    "99": "Network Rail",
}


def toc_name(toc_id: str | None) -> str:
    """Return the operator name for a TRUST TOC code, or 'TOC <id>' if unknown."""
    if toc_id is None:
        return "Unknown"
    return TOC_NAMES.get(toc_id.strip(), f"TOC {toc_id}")
