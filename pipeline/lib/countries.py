"""Canonical country reference for the pipeline.

`country_links` extracted in step 3 sometimes carry values that are not real
countries — subnational regions (Brazilian/US states, Sicily), continents
("Europe", "South America"), UK constituents (England, Scotland) — or the same
country under variant spellings ("Burma" vs "Myanmar", "Ivory Coast" vs
"Côte d'Ivoire"). This module is the single source of truth for:

  * what counts as a real country/territory  (the allowlist), and
  * its one canonical spelling                (alias folding).

Step 4 calls `normalize_country()` on every origin country and country-link so
`crimenet.json` carries clean, de-duplicated country data — and every consumer
(the app, the footprints map, the static pages) inherits it. There is no
display-time filtering anywhere else.

`normalize_country(name)` returns the canonical name, or None if `name` is not a
recognized country (caller drops it). Matching is accent- and case-insensitive
and tolerant of punctuation/whitespace.
"""
from __future__ import annotations

import re
import unicodedata

# canonical name -> alias spellings that should fold into it.
# A country with no variant spellings just maps to itself (empty alias list).
# Widely recognized territories/dependencies that the dataset treats as distinct
# places (Hong Kong, Puerto Rico, …) and a few historical states that appear in
# older sources (East Germany, North/South Vietnam) are included deliberately.
_CANONICAL: dict[str, list[str]] = {
    # ── sovereign UN member states ──
    "Afghanistan": [], "Albania": [], "Algeria": [], "Andorra": [], "Angola": [],
    "Antigua and Barbuda": [], "Argentina": [], "Armenia": [], "Australia": [],
    "Austria": [], "Azerbaijan": [], "Bahamas": ["The Bahamas"], "Bahrain": [],
    "Bangladesh": [], "Barbados": [], "Belarus": [], "Belgium": [], "Belize": [],
    "Benin": [], "Bhutan": [], "Bolivia": [], "Bosnia and Herzegovina": ["Bosnia"],
    "Botswana": [], "Brazil": [], "Brunei": [], "Bulgaria": [], "Burkina Faso": [],
    "Burundi": [], "Cambodia": [], "Cameroon": [], "Canada": [],
    "Cape Verde": ["Cabo Verde"], "Central African Republic": [], "Chad": [],
    "Chile": [], "China": ["People's Republic of China", "PRC"], "Colombia": [],
    "Comoros": [], "Republic of the Congo": ["Congo", "Congo-Brazzaville"],
    "Costa Rica": [], "Croatia": [], "Cuba": [], "Cyprus": [],
    "Czech Republic": ["Czechia"], "Côte d'Ivoire": ["Ivory Coast"],
    "Democratic Republic of the Congo": ["DR Congo", "DRC", "Dem. Rep. Congo",
                                         "Congo-Kinshasa", "Zaire"],
    "Denmark": [], "Djibouti": [], "Dominica": [], "Dominican Republic": [],
    "Ecuador": [], "Egypt": [], "El Salvador": [], "Equatorial Guinea": [],
    "Eritrea": [], "Estonia": [], "Eswatini": ["Swaziland"], "Ethiopia": [],
    "Fiji": [], "Finland": [], "France": [], "Gabon": [], "Gambia": ["The Gambia"],
    "Georgia": [], "Germany": [], "Ghana": [], "Greece": [], "Grenada": [],
    "Guatemala": [], "Guinea": [], "Guinea-Bissau": [], "Guyana": [], "Haiti": [],
    "Honduras": [], "Hungary": [], "Iceland": [], "India": [], "Indonesia": [],
    "Iran": [], "Iraq": [], "Ireland": [], "Israel": [], "Italy": [], "Jamaica": [],
    "Japan": [], "Jordan": [], "Kazakhstan": [], "Kenya": [], "Kiribati": [],
    "Kuwait": [], "Kyrgyzstan": [], "Laos": [], "Latvia": [], "Lebanon": [],
    "Lesotho": [], "Liberia": [], "Libya": [], "Liechtenstein": [], "Lithuania": [],
    "Luxembourg": [], "Madagascar": [], "Malawi": [], "Malaysia": [], "Maldives": [],
    "Mali": [], "Malta": [], "Marshall Islands": [], "Mauritania": [], "Mauritius": [],
    "Mexico": [], "Micronesia": [], "Moldova": [], "Monaco": [], "Mongolia": [],
    "Montenegro": [], "Morocco": [], "Mozambique": [], "Myanmar": ["Burma"],
    "Namibia": [], "Nauru": [], "Nepal": [], "Netherlands": ["Holland"],
    "New Zealand": [], "Nicaragua": [], "Niger": [], "Nigeria": [],
    "North Korea": [], "North Macedonia": ["Macedonia"], "Norway": [], "Oman": [],
    "Pakistan": [], "Palau": [], "Panama": [], "Papua New Guinea": [], "Paraguay": [],
    "Peru": [], "Philippines": [], "Poland": [], "Portugal": [], "Qatar": [],
    "Romania": [], "Russia": ["Russian Federation"], "Rwanda": [],
    "Saint Kitts and Nevis": [], "Saint Lucia": [],
    "Saint Vincent and the Grenadines": [], "Samoa": [], "San Marino": [],
    "Sao Tome and Principe": [], "Saudi Arabia": [], "Senegal": [], "Serbia": [],
    "Seychelles": [], "Sierra Leone": [], "Singapore": [], "Slovakia": [],
    "Slovenia": [], "Solomon Islands": [], "Somalia": [], "South Africa": [],
    "South Korea": [], "South Sudan": [], "Spain": [], "Sri Lanka": [], "Sudan": [],
    "Suriname": [], "Sweden": [], "Switzerland": [], "Syria": [], "Tajikistan": [],
    "Tanzania": [], "Thailand": [], "Timor-Leste": ["East Timor"], "Togo": [],
    "Tonga": [], "Trinidad and Tobago": [], "Tunisia": [], "Turkey": ["Türkiye"],
    "Turkmenistan": [], "Tuvalu": [], "Uganda": [], "Ukraine": [],
    "United Arab Emirates": ["UAE"], "United Kingdom": ["UK", "Britain",
                                                        "Great Britain"],
    "United States": ["United States of America", "USA", "U.S.", "U.S.A.",
                      "America"],
    "Uruguay": [], "Uzbekistan": [], "Vanuatu": [], "Vatican City": ["Holy See"],
    "Venezuela": [], "Vietnam": [], "Yemen": [], "Zambia": [], "Zimbabwe": [],
    # ── widely recognized states / territories kept as distinct places ──
    "Taiwan": [], "Kosovo": [], "Palestine": ["Palestinian territories"],
    "Hong Kong": [], "Macau": ["Macao"], "Puerto Rico": [], "Aruba": [],
    "Curaçao": [], "Northern Cyprus": [], "Greenland": [], "Bermuda": [],
    "Gibraltar": [], "New Caledonia": [],
    # ── historical states that appear in older sources (kept, not merged) ──
    "East Germany": [], "West Germany": [], "North Vietnam": [],
    "South Vietnam": [], "Soviet Union": ["USSR"], "Yugoslavia": [],
    "Czechoslovakia": [],
}


def _fold(s) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", s).strip().lower()


# folded spelling -> canonical name (covers both canonical names and aliases)
_LOOKUP: dict[str, str] = {}
for _canon, _aliases in _CANONICAL.items():
    _LOOKUP[_fold(_canon)] = _canon
    for _a in _aliases:
        _LOOKUP[_fold(_a)] = _canon


def normalize_country(name) -> str | None:
    """Canonical country name for `name`, or None if it isn't a recognized
    country/territory (caller should drop it). Accent/case/spacing-insensitive."""
    return _LOOKUP.get(_fold(name))


def is_country(name) -> bool:
    return _fold(name) in _LOOKUP


# Canonical names, for callers that want the full allowlist.
VALID_COUNTRIES = frozenset(_CANONICAL)
