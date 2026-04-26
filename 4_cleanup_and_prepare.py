"""
cleanup_and_prepare.py
Takes global_network.json → crimenet_specific.json

Usage:
    python 4_cleanup_and_prepare.py --input global_network.json
    python 4_cleanup_and_prepare.py --input global_network.json --stats
"""

import json
import re
import argparse
import logging
from pathlib import Path
from collections import Counter

try:
    import networkx as nx
except ImportError:
    raise ImportError("Install networkx: pip install networkx")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# TYPE CONSOLIDATION
# ═══════════════════════════════════════════════════════════════════

CANONICAL_NODE_TYPES = {"cartel", "mafia", "gang",
    "motorcycle_club", "faction", "clan", "triad", "militia", "terrorist_organization",
}

NODE_TYPE_MAP = {
    # → criminal_organization (catch-all for generic org types)
    "organized_crime_group": "criminal_organization",
    "criminal_group": "criminal_organization",
    "crime_syndicate": "criminal_organization",
    "criminal_syndicate": "criminal_organization",
    "criminal_network": "criminal_organization",
    "organised_crime_group": "criminal_organization",
    # → gang
    "criminal_gang": "gang", "street_gang": "gang", "prison_gang": "gang",
    "hate_group": "gang", "confraternity": "gang",
    # → clan
    "criminal_clan": "clan",
    # → faction
    "criminal_faction": "faction", "armed_wing": "faction", "political_wing": "faction",
    "rebel_faction": "faction",
    # → militia
    "militant_group": "militia", "death_squad": "militia",
    "paramilitary_group": "militia", "paramilitary_organization": "militia",
    "armed_group": "militia", "military_force": "militia",
    "military_organization": "militia", "vigilante_group": "militia",
    "private_military_company": "militia", "criminal_militia": "militia",
    "political_militia": "militia", "insurgent_group": "militia",
    # → cartel
    "drug_cartel": "cartel", "defunct_cartel": "cartel",
    # → gang
    "criminal_cell": "gang", "street_crew": "gang",
    # → organization (catch-all non-criminal)
    "political_organization": "organization", "political_party": "organization",
    "political_alliance": "organization", "coalition": "organization",
    "alliance": "organization", "religious_group": "organization",
    "intelligence_group": "organization", "business_group": "organization",
    "governing_body": "organization", "animal_welfare_organization": "organization",
    # → other existing canonical types
    "yakuza": "criminal_organization",
    "secret_society": "criminal_organization",
    "hacker_group": "criminal_organization",
    "cybercriminal_group": "criminal_organization",
    "cybercriminal_network": "criminal_organization",
    "mafia_group": "mafia",
    "crew": "gang",

}

# ── Detail type consolidation ─────────────────────────────────────
# Canonical detail types for "other" edges
CANONICAL_DETAILS = {
    "splinter", "armed_wing", "successor", "merger",
    "faction_of", "reformation", "founded_by_members_of", "evolved_into",
    "support_club",
}

# Details that mean the edge should be reclassified as alliance (not "other")
DETAIL_TO_ALLIANCE = {
    "alliance", "allied_with", "aligned_with", "collaboration", "cooperation",
    "strategic_alliance", "ties", "contacts", "connections", "links",
    "linked_to", "connected_to", "associated_with", "close_ties",
    "business_partnership", "drug_trafficking_partnership", "pact",
    "cooperated_with", "co-dominant", "co-dominance", "ceasefire_and_cooperation",
    "cooperation_and_extortion", "cooperation_in_smuggling", "forged_ties",
    "forged_relationships", "joint_operation", "underground_ties", "ties_to",
    "working_relationship", "partnered_with", "co-involvement",
    "temporary_alliance", "strategic_alliance", "peace_treaty",
    "drug_trafficking_partner", "drug_supply_connection", "meeting_for_friendship",
    "connected", "linked", "related", "associated_through_blood_relations",
    "family_ties", "personal_ties", "family_connection", "personal_connection",
    "connected_through", "connected_with", "active_with",
}

# Details that mean the edge should be reclassified as rivalry (not "other")
DETAIL_TO_RIVALRY = {
    "rivalry", "conflict", "confrontation", "feud", "competition",
    "targeted_by", "targeted", "retaliation", "armed_conflict",
    "targeted_killing", "assassination", "ordered_hit", "contract_killing",
    "involvement_in_killing", "alliance_turned_rivalry", "alliance_then_conflict",
    "driven_out_by_anti_crime_pact", "financial_dispute", "tension",
    "recruitment_and_conflict", "vanquished", "conquered",
}

# Map noisy detail types → canonical
DETAIL_MAP = {
    # → faction_of
    "member_of": "faction_of", "part_of": "faction_of",
    "affiliated_with": "faction_of", "affiliated_to": "faction_of",
    "affiliated_organization": "faction_of", "affiliated_member": "faction_of",
    "affiliated_gang": "faction_of", "affiliated_support_club": "faction_of",
    "affiliation_of_member": "faction_of",
    "branch_of": "faction_of", "branch": "faction_of",
    "crew_of": "faction_of", "clan_of": "faction_of",
    "subgroup": "faction_of", "subgroup_of": "faction_of",
    "subset": "faction_of", "historical_subset": "faction_of",
    "subordinate": "faction_of", "subordinate_to": "faction_of",
    "cell": "faction_of", "cell_of": "faction_of",
    "component_of": "faction_of", "chapter": "faction_of", "chapter_of": "faction_of",
    "subdivision": "faction_of", "clique": "faction_of", "set": "faction_of",
    "satellite": "faction_of", "represents": "faction_of",
    "affiliate": "faction_of", "affiliate_of": "faction_of",
    "controlled_by": "faction_of", "operated_by": "faction_of",
    "under_authority_of": "faction_of", "governed_by": "faction_of",
    "governing_body": "faction_of", "governing_body_of": "faction_of",
    "rules_over": "faction_of", "oversees": "faction_of", "manages": "faction_of",
    "controls": "faction_of", "control": "faction_of",
    "head_of": "faction_of", "led_by": "faction_of", "leads": "faction_of",
    "co-leads": "faction_of",
    "includes": "faction_of", "group_of": "faction_of",
    "puppet_gang": "faction_of", "puppet_crew": "faction_of",
    "member_group": "faction_of", "member_of_same_faction": "faction_of",
    "represented_by": "faction_of", "represented_in": "faction_of",
    "representation": "faction_of",
    "parent_organization": "faction_of", "faction": "faction_of",
    "front_organization": "faction_of", "front": "faction_of", "legal_front": "faction_of",
    "political_wing": "faction_of", "recruitment_gang": "faction_of",
    "affiliation": "faction_of", "affiliated": "faction_of",
    "grouping": "faction_of",

    # → support_club (MC-specific subordinate)
    "support_club": "support_club", "puppet_club": "support_club",
    "prospect_club": "support_club", "feeder_club": "support_club",
    "hangaround_status": "support_club", "support_gang": "support_club",
    "support_group": "support_club",

    # → merger
    "absorption": "merger", "amalgamation": "merger",
    "incorporated": "merger", "incorporated_into": "merger",
    "merged": "merger", "merged_into": "merger",
    "absorbed": "merger", "absorbed_into": "merger", "absorbed_by": "merger",
    "absorbed_or_disbanded": "merger", "absorbed_members_of": "merger",
    "absorbed_remnants": "merger",
    "patch_over": "merger", "patched_over": "merger", "patching_over": "merger",
    "patch-over": "merger", "patchover": "merger", "members_patch-over": "merger",
    "planned_patch_over_to": "merger", "considered_patching_over": "merger",
    "patching_over_talks": "merger", "merger_talks": "merger",
    "proposed_merger": "merger", "attempted_merger": "merger",
    "merger_attempt": "merger", "merger_of_interests": "merger",
    "consolidated_under": "merger", "attempted_absorption": "merger",
    "joined": "merger",

    # → splinter
    "split": "splinter", "split_off": "splinter",
    "defection": "splinter", "break-away_faction": "splinter",
    "declared_independence": "splinter", "formerly_part": "splinter",
    "formerly_branch": "splinter", "branched_from": "splinter",
    "offshoot": "splinter",

    # → successor
    "precursor": "successor", "predecessor": "successor",
    "historical_predecessor": "successor", "direct_precursor": "successor",
    "took_over": "successor", "took_over_territory": "successor",
    "took_over_rackets": "successor", "took_control_of": "successor",
    "replacement": "successor", "supplanted": "successor",
    "potential_successor": "successor", "forerunner": "successor",
    "heir": "successor", "power_vacuum": "successor",
    "gained_territory_from": "successor", "gained_assets_from": "successor",
    "replaced_by": "successor", "taken_over": "successor", "taken_over_by": "successor",
    "succeeded": "successor", "supersession": "successor",
    "successor_in_market": "successor",

    # → evolved_into
    "evolved_from": "evolved_into", "morphed_from": "evolved_into",
    "transition_to": "evolved_into", "renamed": "evolved_into",
    "re-branding": "evolved_into", "cover_name": "evolved_into",
    "alternative_name": "evolved_into", "alias": "evolved_into",

    # → founded_by_members_of
    "founded_by": "founded_by_members_of", "formed_by": "founded_by_members_of",
    "formed_from_remnants": "founded_by_members_of",
    "created_by": "founded_by_members_of", "spawned": "founded_by_members_of",
    "composed_of_members_of": "founded_by_members_of",
    "formed_by_victims_of": "founded_by_members_of",
    "originated_from": "founded_by_members_of", "outgrowth": "founded_by_members_of",
    "descended_from": "founded_by_members_of", "roots_in": "founded_by_members_of",
    "origins_of": "founded_by_members_of",
    "inspired_by": "founded_by_members_of", "based_on": "founded_by_members_of",
    "influenced_by": "founded_by_members_of",
    "modeled_after": "founded_by_members_of", "modelled_after": "founded_by_members_of",
    "modeled_on": "founded_by_members_of",
    "founded_by_members": "founded_by_members_of",
    "founding_member": "founded_by_members_of",
    "incorporated_members_of": "founded_by_members_of",
    "recruited_from": "founded_by_members_of",

    # → armed_wing
    "paramilitary_wing": "armed_wing", "enforcement_wing": "armed_wing",
    "enforcer_squad": "armed_wing", "enforcer_group": "armed_wing",
    "enforcer_gang": "armed_wing", "enforcement_squad": "armed_wing",
    "youth_wing": "armed_wing", "youth_militia_within": "armed_wing",
}


# ═══════════════════════════════════════════════════════════════════
# GENERIC NODE FILTER
# ═══════════════════════════════════════════════════════════════════

GENERIC_SUFFIXES = [
    r"\bmafia$", r"\borganized crime$", r"\borganised crime$",
    r"\bcrime groups?$", r"\bcriminal organizations?$", r"\bcriminal groups?$",
    r"\bdrug cartels?$", r"\bgangs?$", r"\bcriminal networks?$",
    r"\bcrime syndicate$", r"\bunderworld$", r"\bcriminal underworld$", r"\bmob$",
]

GENERIC_PREFIXES = [
    "african", "albanian", "american", "armenian", "australian", "azerbaijani",
    "azeri", "balkan", "bangladeshi", "belarusian", "bolivian", "bosnian",
    "brazilian", "british", "bulgarian", "burmese", "cambodian", "canadian",
    "caribbean", "central american", "chechen", "chilean", "chinese",
    "colombian", "corsican", "croatian", "cuban", "czech", "dominican",
    "dutch", "east asian", "east european", "ecuadorian", "egyptian",
    "estonian", "european", "filipino", "french", "galician", "georgian",
    "german", "greek", "guatemalan", "haitian", "honduran", "hungarian",
    "indian", "indonesian", "iranian", "iraqi", "irish", "israeli",
    "italian", "jamaican", "japanese", "kazakh", "korean", "kurdish",
    "kyrgyz", "latin american", "latvian", "lebanese", "libyan",
    "lithuanian", "macedonian", "malaysian", "mexican", "moldovan",
    "montenegrin", "moroccan", "nigerian", "north african", "pakistani",
    "palestinian", "paraguayan", "peruvian", "polish", "portuguese",
    "puerto rican", "romanian", "russian", "salvadoran", "saudi",
    "scandinavian", "serbian", "singaporean", "slovak", "south african",
    "south american", "southeast asian", "spanish", "swedish", "swiss",
    "syrian", "taiwanese", "tajik", "thai", "trinidadian", "tunisian",
    "turkish", "turkmen", "ukrainian", "uruguayan", "uzbek", "venezuelan",
    "vietnamese", "west african", "yugoslav",
]

GENERIC_BLOCKLIST = {
    "organized crime", "organised crime", "transnational organized crime",
    "international organized crime", "drug trafficking organizations",
    "drug trafficking", "narcotrafficking", "latin american drug cartels",
    "european organized crime", "asian organized crime", "african organized crime",
    "mafia", "la mafia", "drug cartels", "motorcycle gangs", "drug cartel",
    "criminal organizations", "criminal organization",
    "colombian cartels", "colombian drug cartels", "colombian criminal networks",
    "colombian criminal organizations", "colombian mafia",
    "mexican cartels", "mexican drug cartels",
    "african american gangs", "african criminal networks",
    "chinese gangs", "east asian gangs",
    "dominican criminal groups", "dutch criminal groups",
    "indian gangs", "irish gangs", "italian gangs", "korean gangs",
    "korean criminal groups", "korean criminal organizations",
    "nigerian crime groups", "nigerian organized crime",
    "north african gangs", "puerto rican gangs",
    "russian crime groups", "russian criminal networks", "russian gangs",
    "russian organized crime", "swedish criminal networks",
    "turkish crime groups", "turkish gangs",
    "vietnamese crime groups", "vietnamese gangs",
    "south american drug cartels", "italian crime groups",
    "dutch organized crime", "british underworld",
    "balkan organized crime groups",
}

GENERIC_SAFELIST = {
    "mexican mafia", "new mexican mafia", "irish mob", "dixie mafia",
    "jewish mafia", "cornbread mafia", "black mafia", "black mafia family",
    "thai mafia", "american mafia", "albanian mafia", "serbian mafia",
    "corsican mafia", "israeli mafia", "chechen mafia", "bulgarian mafia",
    "montenegrin mafia", "azerbaijani mafia", "georgian mafia", "armenian mafia",
    "iranian mafia", "lebanese mafia", "kurdish mafia", "turkish mafia",
    "ukrainian mafia", "romanian mafia", "russian mafia", "russian mob",
    "nigerian mafia", "pakistani mafia", "moroccan mafia", "cuban mafia",
    "greek mafia", "indian mafia", "irish mafia", "italian mafia", "italian mob",
    "polish mob", "portuguese mafia", "slovak mafia", "canadian mafia",
    "balkan mafia", "yugoslav mafia", "north macedonian mafia",
    "galician mafia", "red mafia", "new mafia", "dz mafia",
    "axe gang", "31 gang", "856 gang", "b13 gang", "fk gang", "fob gang",
    "k&a gang", "lvm gang", "lal gang", "mbm gang", "sza gang", "sin ma gang",
    "bosnian drug cartel", "cuban drug cartel",
    "georgian organized crime", "israeli organized crime",
}

_SUFFIX_PATTERNS = [re.compile(s, re.IGNORECASE) for s in GENERIC_SUFFIXES]
_PREFIX_SET = set(GENERIC_PREFIXES)


def is_generic_node(name):
    lower = name.strip().lower()
    if lower in GENERIC_SAFELIST:
        return False
    if lower in GENERIC_BLOCKLIST:
        return True
    # Only filter "[Nationality] + [generic suffix]" where prefix is an exact nationality match
    for pattern in _SUFFIX_PATTERNS:
        if pattern.search(lower):
            prefix_part = pattern.sub("", lower).strip().lower()
            if prefix_part in _PREFIX_SET:
                return True
    return False


# ═══════════════════════════════════════════════════════════════════
# KNOWN DUPLICATES (curated, conservative)
# ═══════════════════════════════════════════════════════════════════

KNOWN_DUPLICATES = {
    # ── Italian Mafia ──────────────────────────────────────────────
    "'Ndrangheta": {"'Ndrangheta", "Ndrangheta", "Italian 'Ndrangheta"},
    "Cosa Nostra": {"La Cosa Nostra", "American Cosa Nostra", "Sicilian Cosa Nostra", "Sicilian Mafia"},
    "Corleonesi": {"Corleonesi clan", "Corleonesi Mafia", "Corleonesi Mafia clan"},
    "Camorra": {"Campanian Camorra", "Neapolitan Camorra"},
    "Casalesi clan": {"Casalesi", "Casalesi Camorra clan", "Camorra Casalesi clan"},
    "Casamonica clan": {"Casamonica"},
    "Mazzarella clan": {"Mazzarella", "Clan Mazzarella"},
    "Di Lauro clan": {"Clan Di Lauro"},
    "Moccia clan": {"Clan Moccia"},
    "Nuvoletta clan": {"Clan Nuvoletta"},
    "Sarno clan": {"Clan Sarno"},
    "Contini clan": {"Clan Contino"},
    "Clan Sacco-Bocchetti": {"Sacco-Bocchetti clan", "Bocchetti clan"},
    "Sacra Corona Unita": {"Famiglia Salentina Libera"},
    "Stidda": {"Basilischi"},
    "Union Corse": {"Unione Corse"},
    "Le Milieu": {"Milieu", "French Milieu"},
    "Guérini clan": {"Guerini clan", "Guerini gang"},

    # ── American Mafia ─────────────────────────────────────────────
    "Five Families": {"Five Families of New York", "Five Families of New York City", "New York City's Five Families"},
    "The Commission": {"Mafia Commission", "American Mafia Commission", "Commission"},
    "Bonanno crime family": {"Bonanno family"},
    "Colombo crime family": {"Profaci crime family", "Profaci family"},
    "Gambino crime family": {"Gambino family", "Gambino clan", "Mangano crime family", "Mangano family"},
    "Genovese crime family": {"Genovese family", "Luciano crime family", "Luciano Family"},
    "Lucchese crime family": {"Luchese crime family", "Gagliano crime family", "Gagliano family", "Gagliano-Lucchese family", "Reina crime family", "Reina family", "Reina gang", "Brooklyn faction (Lucchese crime family)"},
    "Morello crime family": {"Morello family", "Morello gang"},
    "DeCavalcante crime family": {"DeCavalcante family"},
    "Rizzuto crime family": {"Rizzuto family"},
    "Cotroni crime family": {"Cotroni family"},
    "Papalia crime family": {"Papalia family", "Papalia clan", "Papalia 'ndrina"},
    "Masseria family": {"Masseria clan", "Masseria crime family", "Masseria faction", "Masseria Mafia"},

    # ── Mexican Cartels ────────────────────────────────────────────
    "Sinaloa Cartel": {"Sinaloa"},
    "Jalisco New Generation Cartel": {"CJNG", "Jalisco Cartel"},
    "Los Zetas": {"Los Zetas Cartel", "Zetas Cartel", "Mexican Los Zetas"},
    "Gulf Cartel": {"Cártel del Golfo"},
    "Beltrán-Leyva Cartel": {"Beltran-Leyva Cartel", "Beltrán Leyva Cartel", "Beltrán-Leyva Organization", "Arturo Beltrán Leyva Organization", "Beltrán Leyva Cartel", "Beltran Leyva Cartel"},
    "Juárez Cartel": {"Juarez Cartel", "New Juárez Cartel", "New Juarez Cartel"},
    "Medellín Cartel": {"Medellin Cartel", "Colombian Medellín Cartel"},
    "Cártel del Noreste": {"Cartel del Noreste"},
    "La Familia Michoacana": {"La Familia Michoacana Cartel", "La Familia Cartel", "La Familia"},
    "Knights Templar Cartel": {"Knights Templar", "Los Caballeros Templarios"},
    "Chapitos": {"Los Chapitos", "Chapitos faction"},
    "Los Cachiros": {"Cachiros"},
    "Tijuana Cartel": {"Mexican Tijuana Cartel"},
    "Clan del Golfo": {"Gulf Clan", "Usuga Clan", "Los Urabeños", "Los Gaitanistas"},
    "La Línea": {"La Linea"},
    "La Oficina de Envigado": {"La Oficina", "Oficina de Envigado", "Oficina", "The Office of Envigado"},
    "Autodefensas Unidas de Colombia": {"United Self-Defense Forces of Colombia", "United Self-Defense Forces of Colombia (AUC)", "AUC", "United Self-Defense Units of Colombia"},
    "Los Rojos": {"Los Rojos Cartel"},

    # ── Colombian/South American ──────────────────────────────────
    "FARC": {"FARC-EP", "Revolutionary Armed Forces of Colombia", "Revolutionary Armed Forces of Colombia (FARC)", "Fuerzas Armadas Revolucionarias de Colombia", "Revolutionary Armed Forces of Colombia – People's Army"},
    "National Liberation Army (Colombia)": {"National Liberation Army (ELN)", "ELN", "UC-ELN"},
    "Primeiro Comando da Capital": {"First Capital Command"},
    "Comando Vermelho": {"Red Command"},
    "Muerte a Secuestradores": {"MAS/Muerte a Secuestradores", "Death to Kidnappers"},
    "Los Extraditables": {"The Extraditables"},
    "Tupamaro": {"Tupamaro Revolutionary Movement", "Túpac Amaru Revolutionary Movement"},
    "La Nueva Familia Michoacana": {"La Nueva Familia Michoacana Organization", "LNFM Cartel"},

    # ── Motorcycle Clubs ───────────────────────────────────────────
    "Hells Angels": {"Hell's Angels", "Hells Angels MC", "Hells Angels Motorcycle Club"},
    "Bandidos": {"Bandidos Motorcycle Club", "Bandidos MC"},
    "Mongols": {"Mongols MC", "Mongols Motorcycle Club"},
    "Outlaws": {"Outlaws MC", "Outlaws Motorcycle Club"},
    "Pagans Motorcycle Club": {"Pagan's", "Pagan's MC", "Pagan's Motorcycle Club", "Pagans"},
    "Comanchero": {"Comanchero Motorcycle Club", "Comancheros"},
    "Rock Machine": {"Rock Machine Motorcycle Club", "Rock Machine MC"},
    "Vagos Motorcycle Club": {"Vagos", "Vagos MC"},
    "Warlocks Motorcycle Club": {"Warlocks", "Warlocks MC"},
    "Diablos Motorcycle Club": {"Diablos MC", "Diablos"},
    "Red Devils Motorcycle Club": {"Red Devils", "Red Devils MC Cologne"},
    "Galloping Goose Motorcycle Club": {"Galloping Goose", "Galloping Goose MC"},
    "Sons of Silence Motorcycle Club": {"Sons of Silence"},
    "Wheels of Soul Motorcycle Club": {"Wheels of Soul"},
    "Highwaymen Motorcycle Club": {"Highwaymen", "Highwaymen MC"},
    "Gypsy Joker Motorcycle Club": {"Gypsy Jokers", "Gypsy Joker Motorcycle Club Australia"},
    "Finks Motorcycle Club": {"Finks"},
    "Rebels Motorcycle Club": {"Rebels"},
    "Nomads Motorcycle Club": {"Nomads", "Nomads bikie gang"},
    "Dirty Dozen Motorcycle Club": {"Dirty Dozen", "Dirty Dozen MC", "Dirty Dozens"},
    "Iron Horsemen Motorcycle Club": {"Iron Horsemen"},
    "Chosen Few Motorcycle Club": {"Chosen Few", "Chosen Few MC"},
    "Satan's Choice Motorcycle Club": {"Satan's Choice", "Satan's Choice MC"},
    "Bacchus Motorcycle Club": {"Bacchus"},
    "Breed Motorcycle Club": {"Breed"},
    "Loners Motorcycle Club": {"Loners", "Loner's motorcycle club"},
    "Black Pistons Motorcycle Club": {"Black Pistons"},
    "Boozefighters Motorcycle Club": {"Boozefighters"},
    "Devil's Disciples Motorcycle Club": {"Devil's Disciples", "Devil's Disciples MC", "Devils Diciples", "Devils Diciples Motorcycle Club", "Devils Disciples"},
    "El Forastero Motorcycle Club": {"El Forastero"},
    "Annihilators Motorcycle Club": {"Annihilators", "Annihilators MC"},
    "Rockers Motorcycle Club": {"Rockers", "Rockers MC", "Rockers Motor Club"},
    "Satan's Angels Motorcycle Club": {"Satan's Angels"},
    "Satudarah MC": {"Satudarah"},

    # ── US Gangs ───────────────────────────────────────────────────
    "18th Street Gang": {"18th Street", "18 Street Gang"},
    "Mara Salvatrucha": {"MS-13", "MS 13", "MS13"},
    "Black Guerrilla Family": {"Black Guerilla Family", "BGF"},
    "Black P. Stones": {"Black P Stones", "Black P. Stone Nation", "Almighty Black P. Stone Nation", "P.R. Stones", "PR Stones"},
    "Gangster Disciples": {"Gangster Disciple Nation", "Black Gangster Disciples", "Black Gangster Disciple Nation", "Black Gangster Disciples Nation", "Black Gangsters Disciples"},
    "Latin Kings": {"Almighty Latin King and Queen Nation"},
    "Vice Lords": {"Almighty Vice Lord Nation"},
    "Simon City Royals": {"Almighty Simon City Royal Nation", "Almighty Simon City Royals", "Simon City"},
    "Latin Eagles": {"Almighty Latin Eagle Nation", "Almighty Latin Eagles Nation", "Almighty Latin Eagle Nation"},
    "Gaylords": {"Almighty Gaylords", "Almighty Gaylords Nation", "Chicago Gaylords"},
    "Folk Nation": {"Folks Nation", "Folks Alliance"},
    "People Nation": {"People Nations"},
    "Sureños": {"Sureño", "Sureños 13", "Sureno", "Surenos"},
    "Nuestra Familia": {"La Nuestra Familia"},
    "Aryan Brotherhood": {"Aryan Brotherhood of Texas"},
    "Armenian Power": {"Armenian Power Gang"},
    "Pink Panthers": {"Pink Panthers gang"},
    "Tango Blast": {"Puro Tango Blast"},
    "Gangster Two-Six": {"Gangster Two Six", "Two-Six", "Two Six", "Two-Sixers", "Two Sixers"},
    "Two-Two Boys": {"Insane Two-Two Nation"},
    "Rollin' 60s Neighborhood Crips": {"Rollin' 60 Neighborhood Crips", "Rolling 60s Neighborhood Crips"},
    "Grape Street Watts Crips": {"Grape Street Crips"},
    "Fruit Town Pirus": {"Fruit Town Piru"},
    "Tree Top Pirus": {"Tree Top Piru"},
    "Numbers Gang": {"Numbers Gangs"},

    # ── Irish/UK ───────────────────────────────────────────────────
    "Provisional Irish Republican Army": {"Irish Republican Army", "Provisional IRA", "IRA", "the IRA"},
    "Continuity Irish Republican Army": {"Continuity IRA"},
    "Real Irish Republican Army": {"Real IRA"},
    "New Irish Republican Army": {"New IRA"},
    "Official Irish Republican Army": {"Official IRA"},
    "Kinahan Organized Crime Group": {"Kinahan Cartel", "Kinahan clan", "Kinahan Organised Crime Group"},
    "Hutch Organized Crime Gang": {"Hutch Gang", "Hutch Organised Crime Gang"},
    "Kray Firm": {"Kray brothers", "Kray Twins", "Kray twins gang", "Kray twins organization", "Kray twins' Firm"},

    # ── Russian/Eastern European ───────────────────────────────────
    "Solntsevskaya bratva": {"Solntsevskaya Bratva", "Solntsevskaya", "Solntsevo Gang"},
    "Tambov Gang": {"Tambovskaya Bratva", "Tambovskaya"},
    "Izmaylovskaya gang": {"Izmailovskaya gang", "Izmailovskaya clan", "Izmaylovskaya clan"},
    "Orekhovskaya gang": {"Orekhovskaya OPG", "Orekhovskaya Organized Crime Group"},
    "Kadyrovtsy": {"Kadyrovites"},

    # ── Asian ──────────────────────────────────────────────────────
    "Yamaguchi-gumi": {"Sixth Yamaguchi-gumi", "The Sixth Yamaguchi-gumi"},
    "Sun Yee On": {"Sun Yee On crew (Wan Chai)", "Sun Yee On faction (Tuen Mun)"},
    "Sumiyoshi-kai": {"Sumiyoshi-ikka"},
    "14K Triad": {"14K", "14K Group"},
    "Sam Gor": {"Sam Gor syndicate"},
    "United Bamboo": {"United Bamboo Gang"},
    "Big Circle Gang": {"Big Circle Boys", "Big Circle"},

    # ── Middle East/Africa ─────────────────────────────────────────
    "Islamic State": {"Islamic State of Iraq and the Levant", "ISIL"},
    "Al Qaeda": {"al-Qaeda"},
    "Al-Shabaab": {"Islamic State in Somalia"},
    "Houthi movement": {"Houthi", "Houthis", "Ansar Allah"},
    "ETA": {"Euskadi Ta Askatasuna", "Basque separatist group ETA"},

    # ── Balkan ─────────────────────────────────────────────────────
    "Šarić clan": {"Šarić's clan", "Šarić gang"},
    "Škaljari clan": {"Škaljar clan"},
    "Kavač clan": {"Kavač"},

    # ── Other ──────────────────────────────────────────────────────
    "Blood & Honour": {"Blood and Honour"},
    "Brazilian militias": {"Brazilian police militias"},
    "Hermandad de Pistoleros Latinos": {"Hermanos Pistoleros Latinos"},
    "Bug and Meyer Mob": {"Bugs and Meyer Mob"},
    "Billy Hill organization": {"Billy Hill's Gang"},
    "Dubois Gang": {"Dubois Brothers", "Dubois Brothers gang"},
    "Hornec gang": {"Hornec crime family", "Hornec family"},
    "Dhak group": {"Dhak crime group", "Dhak Gang", "Dhak group of British Columbia"},
    "Dhak-Duhre group": {"Dhak-Duhre Coalition", "Dhak-Duhre crime groups", "Dhak-Duhre gang"},
    "Gooch gang": {"Gooch Close Gang"},
    "Doddington Gang": {"Doddington Close gang", "Doddington Original Gangsters"},
    "Black September": {"Black September Movement"},
    "Bouyakhrichan organization": {"Bouyakhrichan organisation"},
    "Ait Soussan organization": {"Ait Soussan organisation"},
    "Năm Cam's organization": {"Năm Cam's Gang"},
    "Đại Cathay's gang": {"Đại Cathay's organization"},
    "Cuntrera-Caruana Mafia clan": {"Cuntrera-Caruana clan"},
    "Al-Zein crime family": {"Al Zein Clan"},
    "Alameddine Crime Family": {"Alameddine crime network"},
    "Abergil crime family": {"Abergil Organization"},
    "Grave Yard Gangster Crips": {"Grave Yard Gangsta Crips"},
    "Westies": {"Westies gang", "The Westies"},
    "Albanian Mafia": {"Albanian mafia families"},
    "Galician mafia": {"Galician mafias"},

    # ── al-Qaeda affiliates ────────────────────────────────────────
    "al-Qaeda in the Arabian Peninsula": {"AQAP"},
    "al-Qaeda in the Islamic Maghreb": {"AQIM"},
    "Jama'at Nusrat al-Islam wal-Muslimin": {"Nusrat al-Islam"},
 
    # ── Abu Nidal ──────────────────────────────────────────────────
    "Abu Nidal Organization": {"Abu Nidal"},
 
    # ── PKK (political org + rebrands) ─────────────────────────────
    "Kurdistan Workers' Party": {
        "PKK",
        "Kurdish terrorist PKK",
        "Kurdistan Freedom and Democracy Congress",   # KADEK rebrand (2002)
        "People's Congress of Kurdistan",             # KONGRA-GEL rebrand (2003)
    },
 
    # ── PKK armed wings (successive renamings) ────────────────────
    "Kurdistan Freedom Brigades": {
        "Kurdistan Liberation Force",                 # both = HRK (1984)
    },
    "Kurdistan People's Liberation Force": {
        "People's Liberation Army of Kurdistan",      # both = ARGK (1986)
    },
    "People's Defence Forces": {
        "People's Defense Forces",                    # British vs American spelling of HPG
    },
 
    # ── PKK political fronts ──────────────────────────────────────
    # (Kurdistan Communities Union, National Liberation Front of Kurdistan,
    #  Patriotic Revolutionary Youth Movement are distinct wings — keep separate)
 
    # ── Red Army Faction ──────────────────────────────────────────
    "Red Army Faction": {"Rote Armee Fraktion"},
 
    # ── Honduras death squad ──────────────────────────────────────
    "Battalion 3-16": {"Battalion 316"},
 
    # ── Puerto Rican nationalists ─────────────────────────────────
    "Fuerzas Armadas de Liberación Nacional": {"FALN"},
 
    # ── ULFA ──────────────────────────────────────────────────────
    "United Liberation Front of Asom": {"United Liberation Front of Assam"},
 
    # ── Iraqi Shia militia ────────────────────────────────────────
    "Harakat al-Nujaba": {"Harakat Hezbollah al-Nujaba"},
 
    # ── FARC dissidents ───────────────────────────────────────────
    "FARC dissidents": {"FARC-EP dissidents"},        # same phenomenon, different label
 
    # ── Somali Islamist governance ────────────────────────────────
    "Islamic Courts Union": {"Islamic Court Union"},

        # ── cartel duplicates ──────────────────────────────────────────
    "BACRIM": {"BACRIMs"},
    "Los Ántrax": {"Los Antrax"},
    "Black Eagles": {"The Black Eagles"},
    "Los Blancos de Troya": {"Los Blancos De La Troya"},
    "Taghi organization": {"Taghi organisation", "Organisation of Taghi"},
    "Uzbek network": {"Uzbek criminals network"},
    "Norte del Valle cartel": {"North Valley"},
    "Cartel of the Suns": {"Venezuela Cartel of the Suns"},
    "Cali Cartel": {"Colombian Cali cartel"},
    "Cártel Independiente de Acapulco": {"Independent Cartel of Acapulco"},
 
    # ── faction duplicates ─────────────────────────────────────────
    "King Motherland Chicago": {"King Motherland Chicago faction"},
    "Kantō Hatsuka-kai": {"Kanto Hatsuka-kai"},
    "Lucchese crime family New Jersey faction": {"Lucchese-New Jersey faction"},
    "Los Metros": {"Metros"},
    "Bloodline": {"Bloodline faction"},
    "Maceo Organization": {"Maceo Syndicate"},
    "ACDEGAM": {"Asociación Campesina de Ganaderos y Agricultores del Magdalena Medio"},
 
    # ── Indonesian duplicates ──────────────────────────────────────
    "Pemuda Pancasila": {"Pancasila Youth", "Patriot Party (Pemuda Pancasila)"},

        # ── gang ───────────────────────────────────────────────────────
    "Eight Tray Gangster Crips": {"Eight Trey Gangster Crips", "Eight Trey Gangster Crip"},
    "South Side Compton Crips": {"Southside Compton Crips"},
    "Down River Gang": {"Downriver gang"},
    "Frog Town Rifa": {"Frogtown Rifa"},
    "West Side Piru": {"Westside Piru"},
    "United Nations (gang)": {"United Nations gang"},
    "Wolf Pack": {"Wolfpack"},
    "Chambers Brothers": {"The Chambers Brothers"},
    "Chaddi Baniyan Gang": {"Chaddi Baniyan Gangs"},
    "East Side Longo": {"East Side Longos"},
    "Cuckoo Gang": {"Cuckoos Gang"},
    "Black Axe": {"Black Axes"},
    "Mad Cowz": {"Madcowz"},
    "Yardie": {"Yardies"},
    "Satan Disciples": {"Satan's Disciples"},
    "H Block": {"H-Block"},
    "Vietnamese Boyz": {"Vietnamese Boyz Gang"},
    "Bloods alliance": {"Bloods gang alliance"},
    "The Rascals": {"Thee Rascals"},
    "Rollin 90's Neighborhood Crips": {"Rollin' 90s Neighborhood Crips"},
    "Maghreb gang": {"Maghrebian gangs"},

    # ── mafia ──────────────────────────────────────────────────────
    "Gōda-ikka": {"Goda-ikka"},
    "Santa Maria di Gesù Mafia family": {"Santa Maria di Gesù Family"},
    "New England crime family": {"New England family"},
    "Philadelphia crime family": {"Philadelphia family"},
    "Lebanese mafia": {"Lebanese mafias"},

    # ── clan ───────────────────────────────────────────────────────
    "Hamze Crime Family": {"Hamzy/Hamze crime family"},
    "Kutaisi Clan": {"Kutaisi criminal group"},
    "Tbilisi Clan": {"Tbilisi criminal group"},
    "Misso clan": {"Missos clan"},
    "Pettingill family": {"Pettingill families"},
    "Miri clan": {"Miri-Clan"},

    # ── triad ──────────────────────────────────────────────────────
    "Tong": {"Tongs"},
    "Yao Lai": {"Yau Lai"},

    # 3. TYPOS (not removals, just fixes)
    "12th Street Players": {"12st Players"},

    # ── faction ────────────────────────────────────────────────────
    "Sicilian Mafia faction (Newark)": {"Sicilian faction (Newark)"},

    # ── motorcycle_club ────────────────────────────────────────────
    "Demon Keepers Motorcycle Club": {"Demon Keepers MC", "Demons Keepers"},
    "Black Diamond Riders Motorcycle Club": {"Black Diamond Riders", "Black Diamond Riders MC"},
    "13th Tribe MC": {"13th Tribe"},
    "Vagabonds MC": {"Vagabonds"},
    "Evil Ones MC": {"Evil Ones"},
    "Death Riders MC": {"Death Riders"},
    "Satan Slaves MC": {"Satans Slaves"},
    "Jokers MC": {"Joker MC"},
    "Los Bravos": {"Los Brovos"},
    "Midlands Outlaws": {"Midland Outlaws"},
    "King's Crew Motorcycle Club": {"Kings Crew Motorcycle Club"},
    "Popeyes Motorcycle Club": {"Popeye Motorcycle Club"},
    "Queensmen Motorcycle Club": {"Queensman Motorcycle Club"},
    "Kingsmen Motorcycle Club": {"Kinsmen Motorcycle Club"},
    "Original Red Devils Motorcycle Club": {"Red Devils Motorcycle Club"},

    # Jewish Mob appears 3 times
    "Jewish Mob": {"Jewish-American mob", "Jewish-American organized crime"},
 
    # Irish-American umbrella overlaps with Irish Mafia
    "Irish Mafia": {"Irish-American organized crime"},

    "Yakuza": {"Japanese yakuza"},

}

# ═══════════════════════════════════════════════════════════════════
# URL HELPERS
# ═══════════════════════════════════════════════════════════════════

def is_valid_wiki_url(url):
    return url and ("wikipedia.org/" in url)

def extract_wiki_title(url):
    if not url:
        return None
    # Format 1: /w/index.php?title=Article_Name&oldid=...
    match = re.search(r'title=([^&]+)', url)
    if match:
        raw = match.group(1)
    # Format 2: /wiki/Article_Name
    elif '/wiki/' in url:
        raw = url.split('/wiki/')[-1].split('?')[0].split('#')[0]
    else:
        return None
    try:
        from urllib.parse import unquote
        raw = unquote(raw)
    except ImportError:
        raw = raw.replace('%27', "'").replace('%28', '(').replace('%29', ')')
    return raw.replace('_', ' ')


def split_node_sources(node_name, aliases, urls):
    name_lower = node_name.strip().lower()
    alias_set = {a.strip().lower() for a in aliases} if aliases else set()
    alias_set.add(name_lower)

    # Generic/umbrella page titles that should never be assigned as own_source
    GENERIC_SOURCE_TITLES = {
        "mafia", "gang", "cartel", "triad", "organized crime", "crime family",
        "death squad", "irish mob", "bloods", "crips", "yakuza",
    }

    # Hardcoded correct sources for nodes where auto-matching fails
    SOURCE_OVERRIDES = {
        "cosa nostra": {"url": "https://en.wikipedia.org/w/index.php?title=Sicilian_Mafia&oldid=1343334461", "title": "Sicilian Mafia"},
        "bloods": {"url": "https://en.wikipedia.org/wiki/Bloods", "title": "Bloods"},
        "evil corp": {"url": "https://en.wikipedia.org/wiki/Evil_Corp", "title": "Evil Corp"},
        "joe boys": {"url": "https://en.wikipedia.org/wiki/Joe_Boys", "title": "Joe Boys"},
        "gallo crew": None,      # No dedicated Wikipedia page
        "wo group": None,         # No dedicated Wikipedia page
    }

    if name_lower in SOURCE_OVERRIDES:
        own_source = SOURCE_OVERRIDES[name_lower]
        mentioned_in = [{"url": u, "title": extract_wiki_title(u) or "Wikipedia"} for u in urls if not own_source or u != own_source["url"]]
        return own_source, mentioned_in

    own_source = None
    mentioned_in = []

    for url in urls:
        title = extract_wiki_title(url)
        if not title:
            mentioned_in.append({"url": url, "title": "Wikipedia"})
            continue
        title_lower = title.strip().lower()

        # Never assign generic umbrella pages as own_source
        if title_lower in GENERIC_SOURCE_TITLES:
            mentioned_in.append({"url": url, "title": title})
            continue

        is_own = False
        # Exact match: "Mexican Mafia" == "Mexican Mafia"
        if title_lower == name_lower:
            is_own = True
        # Alias match: title matches a known alias
        elif title_lower in alias_set:
            is_own = True
        # Org name appears in article title (e.g. "Hells Angels" in "Hells Angels MC criminal allegations")
        # But only if org name is substantial (>50% of title), to avoid "Gang" matching everything
        elif name_lower in title_lower and len(name_lower) > len(title_lower) * 0.5:
            is_own = True

        if is_own and own_source is None:
            own_source = {"url": url, "title": title}
        else:
            mentioned_in.append({"url": url, "title": title})

    return own_source, mentioned_in


# ═══════════════════════════════════════════════════════════════════
# BETWEENNESS CENTRALITY
# ═══════════════════════════════════════════════════════════════════

def compute_betweenness(node_names, edge_list):
    # Only include nodes that have at least one edge in this subgraph
    connected = set()
    for e in edge_list:
        s, t = e.get("source", ""), e.get("target", "")
        if s in node_names and t in node_names and s != t:
            connected.add(s)
            connected.add(t)

    G = nx.Graph()
    G.add_nodes_from(connected)
    for e in edge_list:
        s, t = e.get("source", ""), e.get("target", "")
        if s in connected and t in connected:
            G.add_edge(s, t)

    n = len(G.nodes)
    if n < 2:
        return {name: 0.0 for name in node_names}

    log.info(f"  Betweenness centrality ({n} connected nodes, {G.number_of_edges()} edges)…")
    bc = nx.betweenness_centrality(G, normalized=True)

    # Fill in 0 for disconnected nodes
    for name in node_names:
        bc.setdefault(name, 0.0)

    top5 = sorted(bc.items(), key=lambda x: -x[1])[:5]
    log.info(f"  Top 5: {[(n, round(v, 4)) for n, v in top5]}")
    return bc

# ═══════════════════════════════════════════════════════════════════
# DEDUP
# ═══════════════════════════════════════════════════════════════════

def normalize(name):
    return re.sub(r"\s+", " ", name.strip().lower())


def build_dedup_map(nodes):
    variant_to_canonical = {}
    for canonical, variants in KNOWN_DUPLICATES.items():
        for v in variants:
            variant_to_canonical[v.strip().lower()] = canonical
        variant_to_canonical[canonical.strip().lower()] = canonical

    merge_map = {}
    group_tracker = {}

    for node in nodes:
        name = node["standard_name"]
        lower = name.strip().lower()
        if lower in variant_to_canonical:
            canonical = variant_to_canonical[lower]
            if name != canonical:
                merge_map[name] = canonical
                if canonical not in group_tracker:
                    group_tracker[canonical] = {canonical}
                group_tracker[canonical].add(name)

    groups = [v for v in group_tracker.values() if len(v) > 1]
    return merge_map, groups


def consolidate_node_type(t):
    t = t.strip().lower()
    if t in CANONICAL_NODE_TYPES:
        return t
    return NODE_TYPE_MAP.get(t, "criminal_organization")


def consolidate_detail(d):
    if not d:
        return None
    d = d.strip().lower()
    if d in CANONICAL_DETAILS:
        return d
    return DETAIL_MAP.get(d, d)


# ═══════════════════════════════════════════════════════════════════
# CLEANUP PIPELINE
# ═══════════════════════════════════════════════════════════════════

def cleanup(data, show_stats=False):
    nodes = data.get("nodes", [])
    edges = data.get("edges", [])
    log.info(f"Input: {len(nodes)} nodes, {len(edges)} edges")

    # 1. Consolidate node types
    tc = 0
    for node in nodes:
        old = node.get("type", "")
        new = consolidate_node_type(old)
        if old != new:
            tc += 1
        node["type"] = new
    log.info(f"Node type consolidation: {tc} remapped")

    TO_BE_EXCLUDED = {
        "civil police", "military police", "criminal investigation department",
        "grupo astra", "kenyan anti-terrorism police unit", "legion of frontiersmen",
        "military reaction force", "puntland maritime police force",
        "bloque de búsqueda",
        "armed forces of the democratic republic of the congo",
        "fsb", "kgb", "nkvd", "ogpu",
        "chinese army", "russian armed forces", "russian ministry of defense",
        "libyan army", "national guard of georgia",
        "combined task force 150", "combined task force 151",
        "c10", "bureau 121", "academi",
        "south african communist party", "african national congress",
        "black lives matter", "government of national accord",
        "national transitional council", "sarekat islam",
        "lebanese kataeb party", "british national party",
        "democracia nacional", "arrow cross party",
        "communist party of burma", "communist party of arakan",
        "communist party of ecuador – red sun",
        "communist party of nepal (maoist centre)",
        "communist party of peru – huallaga regional committee",
        "communist party of peru – red mantaro base committee",
        "militarized communist party of peru",
        "laborers' international union of north america local 210",
        "united motorcycle council of nsw",
        "sociedad albizu campos",
        "alpa investment l.l.c", "alpa trading – fzco",
        "chesed shel emes", "cao đài", "believing youth",
        "general association of korean residents in japan",
        "cultural association of latin kings and queens of catalonia",
        "tammany hall", "posse comitatus",
        "carderplanet.com", "cardersmarket",
        "ustaše", "irgun", "einsatzgruppen", "cossacks",
        "estados unidos",
        "islamic republic of iran",
        "qatar",
        "ba'athist syria",
        "libyan arab jamahiriya",
        "mexican army",
        "syrian army",
        "sudanese armed forces",
        "imperial japanese army",
        "iran's ministry of defense and armed forces logistics",
        "dirección federal de seguridad",
        "jamaica labour party",
        "black panther party",
        "students for a democratic society",
        "indonesian communist party",
        "rescue ink",
        "oscar foundation free legal aid clinic-kenya",
        "300 do brasil", # Political activist group [cite: 4]
        "81st special forces brigade", # Formal military unit of the Nigerian Army [cite: 5]
        "air force intelligence branch militias", # Branch of the Syrian state intelligence apparatus [cite: 8]
        "alperen ocakları", # Cultural and educational foundation/youth organization [cite: 11]
        "amal movement", # Major Lebanese political party [cite: 12]
        "american nazi party", # Formal political party [cite: 12]
        "auxiliary division of the ric", # Historical state-sanctioned paramilitary police force [cite: 18, 19]
        "azov regiment", # Formal unit of the National Guard of Ukraine [cite: 19]
        "brazilian integralist action", # Historical political movement [cite: 31]
        "buryat soldiers", # Regular ethnic soldiers of the Russian Armed Forces [cite: 34]
        "civil defense units", # Kurdish self-defense administrative units [cite: 42]
        "frelimo", # Ruling political party of Mozambique [cite: 72]
        "gestapo", # Official secret police of Nazi Germany [cite: 77]
        "gru", # Russian military intelligence agency [cite: 81]
        "katyń motorocycle raid", # Patriotic/commemorative motorcycle rally [cite: 101]
        "mpla", # Ruling political party of Angola [cite: 131]
        "myanmar military", # Official state armed forces of Myanmar [cite: 132]
        "nation of islam", # Religious and political movement [cite: 133]
        "national defense forces", # Pro-government branch of the Syrian Armed Forces [cite: 133]
        "photography workshop", # Non-criminal professional/artistic gathering [cite: 150]
        "rosgvardia", # National Guard of the Russian Federation [cite: 163]
        "stasi", # Official state security service of East Germany [cite: 179]
        "sturmabteilung", # Historical paramilitary wing of the Nazi Party [cite: 180]
        "swapo", # Ruling political party of Namibia [cite: 182]
        "street transvestite action revolutionaries", # Historical civil rights organization [cite: 180]
        "myanmar national democratic alliance army",
        "myanmar nationalities democratic alliance army",
        "rwandan defence force",              # National military
        "islamic revolutionary guard corps",  # National military branch
        "army of republika srpska",           # Official state military (historical)
        "bolivian military junta",            # National government/military leadership
        "rapid action battalion",             # State law enforcement (Bangladesh)
        "chechen omon",                       # State law enforcement (Russia)
        "special action forces",              # State law enforcement (Philippines)
        "cocula municipal police",            # Local government police
        "iguala municipal police",            # Local government police
        "palestine liberation organization",  # Legitimate political/governing entity
        "great union party (bbp)",            # Registered political party
        "partido socialista puertorriqueño",  # Registered political party (historical)
        "anti-government groups",
        "alliance",
        "asian street gangs",
        "british firms",
        "white panther party",
        "drug syndicate",
        "bolsheviks",
        "việt minh",
        "boxer rebellion",
        "european syndicates",
        "umkhonto we sizwe",
        "polisario front",
        "libyan national army",
        "republic of artsakh armed forces",
        "african-american organized crime groups",
        "albanian groups",
        "chinese criminal organisations",
        "drug trafficking organizations in the us",
        "albanian mobsters",
        "syrian democratic forces",
        "civic action service",
        "boxers",
        "24",
        "african-american drug gangs",
        "continental press service",
        "burnaby criminal gangs",
        "central/eastern european crime groups",
        "criminal syndicate",
        "drug cartels in mexico",
        "oprichniki",
        "counterfeitlibrary.com",
        "british crime firms",
        "eastern european mafias",
        "ethnic chinese criminal groups",
        "civilian vigilante and militia groups",
        "arab criminal family",
        "armenian crime figures",
        "young patriots organization",
        "afghan criminals",
        "36",
        "civil protections units",
        "young lords",
        "emerging group",
        "drug cartels in italy",
        "arab street gangs",
        "asian-based networks",
        "taiping heavenly kingdom",
        "08",
        "cartel",
        "businessmen",
        "falangists",
        "black panthers",
        "diehard duterte supporters",
        "east europeans",
        "anti-terrorist centers",
        "asian, hispanic, and black gangs",
        "chinese criminal syndicates",
        "drug trafficking organizations in brazil",
        "chinese organized criminal groups",
        "drug trafficking organizations in colombia",
        "criminal band connected to elmuraz mamedov",
        "chechen republic of ichkeria",
        "chechen republic of ichkeria militants",
        "axis of resistance",
        "drug trafficking organizations in mexico",
        "asian gangs",
        "allied groups",  # generic label
        "armed groups",  # generic category
        "drug traffickers in latin america",  # generic category
        "black street gangs",  # generic category
        "black prison and street gangs",  # generic category
        "eastern european, persian, or italian based groups",  # vague umbrella
        "east asian street gangs",  # generic category
        "east asian (chinese/vietnamese) street gangs",  # generic category
        "african-american mob",  # generic descriptor
        "asian syndicates",  # generic umbrella
        "balkans criminal organizations",  # generic category
        "austrian criminal group",  # vague/non-specific
        "bari crime groups",  # generic
        "cape verdean organized crime",  # generic category
        "colombian gangsters",  # generic category
        "colombian cocaine mafias",  # generic category
        "colombian drug trafficking groups",  # generic category
        "chinese mafia in europe",  # generic umbrella
        "chinese tongs",  # umbrella term
        "chinese triads",  # umbrella term
        "dacoits",  # generic term
        "dacoit bands",  # generic category
        "dacoit gangs of burdwan",  # regional category
        "dacoit gangs of nadia and hooghly",  # regional category
        "drug traffickers in latin america",  # generic category (duplicate kept intentionally if present twice)
        "eastern black sea crime groups",  # vague regional grouping
        "balkans criminal organizations",  # duplicate umbrella
        "austrian criminal group",  # duplicate vague label
        "bari crime groups",  # duplicate
        "cape verdean organized crime",  # duplicate
        "colombian drug trafficking groups",  # duplicate
        "east asian street gangs",  # duplicate
        "thuggee and dacoity department",
        "transitional federal government",
        "nationwide ceasefire coordination team",
        "people's national party",
        "pan-africanist congress",
        "polynesian panthers",
        "kaloti jewellery international",
        "federated ship painters and dockers union",
        "puerto rican nationalist party",
        "national association of seadogs",
        "belçika türk federasyonu (btf)",
        "1850 washington st",
        "banjaras",
        "iranian intelligence agencies",
        "juggalos",
        "maratha armies",
        "nagas",
        "Various gangs in New York City",
        "Various gangs",
        "various gangs",
        "de norsemen kclub of nigeria",
        "orang laut pirate crews",
        "various latin american drug trafficking organizations",
        'mcreary crime family',
        "mora_001",  
        "akira", 
        "alphv", 
        "lazarus group", 
        "lockbit", 
        "maze", 
        "ransomhub", 
        "revil", 
        "scattered spider", 
        "shadow brokers",  
        "evil corp",
        "darkmarket",
        "guardias de la paz",
        "pembela tanah air",
        "rurales",
        "tiradores de altura",
        "the enterprise",
        "christian front",
        "alperen hearths",
        "bosnian serb army",
        "elohim city",
        "triads",
        "nigerian mafia",
        "hayashi's yakuza",
        "traditional organized crime groups", 
        "west coast street gangs",
        "jamaican posses",
        'official irish republican army',
        "arakan army",
        "arakan liberation army",
        "arakan rohingya salvation army",
        "black army",
        "black liberation army",
        "boricua popular army",
        "chin liberation army",
        "chin national army",
        "free syrian army",
        "god's army",
        "kachin independence army",
        "karen national liberation army",
        "karenni army",
        "kosovo liberation army",
        "kuki national army",
        "mon national liberation army",
        "mong tai army",
        "national army of liberation",
        "national democratic alliance army",
        "national liberation army",
        "official irish republican army",
        "pa-o national liberation army",
        "people's guerrilla army",
        "people's liberation army",
        "popular liberation army",
        "popular revolutionary army",
        "red army",
        "revolutionary army of congo",
        "revolutionary people's army",
        "shan state army",
        "shan state army – north",
        "shan state army – south",
        "shan state independence army",
        "shan state national army",
        "shan united revolutionary army",
        "south lebanon army",
        "syrian national army",
        "ta'ang army",
        "ta'ang national liberation army",
        "united front and people's guerrilla army",
        "united wa state army",
        "wa national army",
        "zapatista army of national liberation",
        "zomi revolutionary army",
        "afc-m23",
        "africa corps",
        "ahlu sunna waljama'a",
        "akhmat units",
        "al-bara battalion",
        "alliance des patriotes pour un congo libre et souverain",
        "alliance nationale des congolais pour la défense des droits de l'homme",
        "anti-sandinista guerrilla special forces",
        "api",
        "arde frente sur",
        "ashab al-kahf",
        "auto-défense des communautés victimes de l'ituri",
        "autodefensas",                         # mexican self-defense origin (not AUC)
        "awb",
        "babylon brigades",
        "badr organization",
        "bara",
        "bears brigade",
        "black and tans",
        "black hundreds",
        "botanical youth club",
        "bushwhackers",
        "carlingue",
        "chechen death battalion",
        "chinland defence force",
        "coalition des mouvements pour le changement/forces de défense du peuple",
        "coalition of patriots for change",
        "colectivos",
        "congolese rally for democracy–goma",
        "contras",
        "coopérative pour le développement du congo/union des révolutionnaires pour la défense du peuple congolais",
        "corak kata katanga",
        "cossacks paramilitary group",
        "counter-guerrilla",
        "cristeros",
        "democratic forces for the liberation of rwanda",
        "desert wolves",
        "dev-genç",
        "espanola formation",
        "far-ept",
        "fbl",
        "fifteenth of september legion",
        "force de résistance patriotique de l'ituri",
        "forces démocratiques de libération du rwanda",
        "forces républicaines fédéralistes",
        "fourth armored division militias",
        "free aceh movement",
        "freikorps",
        "friends stand united",
        "front pembela islam",
        "g-pèp",
        "gcp",
        "grey wolves brigades",
        "group of popular combatants",
        "grupos de autodefensa comunitaria",
        "guerrero state militia",
        "hamot",
        "islamic arab legion",
        "islamic legion",
        "islamist insurgents",
        "jayhawkers",
        "jem",
        "joint darfur force",
        "kadyrovtsy",
        "karen national union",
        "karenni national people's liberation front",
        "karenni national progressive party",
        "karenni nationalities defence force",
        "karenni people's defence force",
        "kata'ib al-imam ali",
        "kata'ib jund al-imam",
        "katanga tigers",
        "kayan new land party",
        "knu",
        "kokang force",
        "lahu democratic union",
        "lebanese brigades to fight the israeli occupation",
        "lebanese resistance brigades",
        "liwa fatemiyoun",
        "local defense forces",
        "lrjr",
        "luhansk people's republic",
        "m27",
        "maguindanao guerrillas",
        "mai-mai",
        "mai-mai cheka",
        "mai-mai gedeon",
        "mai-mai kata katanga",
        "mai-mai militias",
        "mai-mai pareco",
        "mai-mai yakutumba",
        "mapi",
        "march 23 movement",
        "maskhadov government troops",
        "mau mau",
        "milice",
        "milicias populares anti-sandinistas",
        "military security shield forces",
        "militia of montana",
        "minuteman project",
        "misura",
        "misurasata",
        "mndaa",
        "moro national liberation front",
        "mountain church of jesus christ the savior",
        "mouvement de lutte contre l'agression au zaïre/forces unies de résistance nationale contre l'agression de la république démocratique du congo",
        "mpcp",
        "mudundu 40/front de résistance et de défense du kivu",
        "muntasir billah brigade",
        "national congress for the defence of the people",
        "nawaphon",
        "nduma défense du congo-rénové",
        "new jersey minutemen",
        "new mon state party",
        "nicaraguan contras",
        "nicaraguan democratic force",
        "nicaraguan democratic union",
        "nicaraguan resistance",
        "non-state groups in kashmir",
        "noom suk harn",
        "northern alliance",
        "nyatura",
        "og imba",
        "pan-african legion",
        "paraguayan people's army",
        "paramilitary groups",
        "patriot",
        "patriotes résistants congolais",
        "pañuelo negro",
        "pdf",
        "peasant self-defense forces of casanare",
        "pedro león arboleda movement",
        "people against gangsterism and drugs",
        "people's defence force",
        "popular mobilization forces",
        "popular resistance of sudan",
        "pyatnashka international brigade",
        "quds force",
        "quintín lame armed movement",
        "radwan force",
        "rally for congolese democracy–goma",
        "rally for congolese democracy–kisangani/movement for liberation",
        "rassemblement congolais pour la démocratie–goma",
        "raïa mutomboki",
        "red flag communist party",
        "red gaurs",
        "red guards",
        "red shirts",
        "redut",
        "restoration council of shan state",
        "revolutionary forces",
        "right-wing paramilitaries",
        "russian cossack formations",
        "russian imperial legion",
        "russian separatist forces in donbas",
        "sandinista revolutionary front",
        "saraya al-salam",
        "saraya awliya al-dam",
        "saraya khorasani",
        "serbian paramilitaries in kosovo",
        "sever",
        "shabiha",
        "shan national united front",
        "slm (al-nur)",
        "slm (minnawi)",
        "slm (tambour)",
        "somali national movement",
        "somali salvation democratic front",
        "sons of liberty",
        "special groups",
        "ssa-s",
        "sudanese revolutionary front",
        "surrendered ulfa",
        "tajammu al-arabi",
        "tamazuj",
        "the covenant, the sword, and the arm of the lord",
        "tiger forces",
        "tnla",
        "tuareg rebels",
        "tupamaro",
        "tupamaro revolutionary movement", 
        "túpac amaru revolutionary movement",
        "turkmen abdulhamid han brigade",
        "ulster loyalists",
        "ulster protestant volunteers",
        "union des patriotes congolais",
        "united nicaraguan opposition",
        "upc rebels",
        "veren laki",
        "village scouts",
        "vostok battalion",
        "wagner group",
        "wazalendo",
        "wazalendo burundi",
        "yatama",
        "yug",
        "zanla",
        "zhang zuolin's forces",
        "zuwayya brigades",
        "fatah",                                        # ruling party of the Palestinian Authority, not designated terrorist by US/EU/UN
        "farabundo martí national liberation front",    # became a legitimate political party in 1992, governed El Salvador 2009-2019
        "19th of april movement",                       # demobilized 1990, became the M-19 Democratic Alliance political party
        "m-19",                                         # same as above (duplicate entry)
        "movimiento 19 de abril",                       # same as above (duplicate entry)
        "armed forces of national liberation",          # defunct 1960s Venezuelan guerrilla, not designated terrorist
        "fenians",                                      # 19th-century Irish republican political movement, historical
        "people's will",                                # 19th-century Russian revolutionary org, historical
        "bolshevik battle squad",                       # pre-revolutionary Russian group, historical
        "rotfrontkämpferbund",                          # Weimar-era Communist paramilitary, historical militia
        "organisation consul",                          # Weimar-era ultra-nationalist group, historical faction
        "combat brigade of the socialist revolutionary party",  # Tsarist-era death squad, historical faction
        "terrorist or secessionist groups",             # generic descriptor, NOT an actual organization
        "islamic militants",                            # generic descriptor, NOT an actual organization
        "peruvian communist party",
        "peruvian communist party – red flag",
        "latin american defense organization",
        "movement pro independence",
        "puerto rican revolutionary workers organization",
        "frente integralista brasileira",
        "movimento integralista e linearista brasileiro",
        "german american bund",
        "movimiento femenino popular",
        "revolutionary internationalist movement",
        "hòa hảo",
        "church of jesus christ–christian",
        "federation of turkish democratic idealist associations in germany",
        "federation of the world order in europe",
        "turkish federation netherlands",
        "turkish islamic federation",
        "union of turkish-islamic cultural associations in europe",
        "swedish grey wolves",
        "federaciones cocaleras del trópico",
        "federación especial de colonizadores de chimoré",
        "simón bolívar guerrilla coordinating board",
        "splm-n (agar)",
        "splm-n (al-hilu)",
        "irish northern aid committee",
        "local clans and families",
        "regional clans",
        "warlords",
        "smaller drug gangs",
        "smaller regional gangs",
        "others flats gangs",
        "various gangs in new york city",
        "neo-nazi and nationalist groups",
        "organised crime syndicates",
        "organized crime in southeast asia",
        "hemispheric drug syndicates",
        "independent traffickers",
        "international cartels",
        "south american cartels",
        "paraguayan cartels",
        "uruguayan cartels",
        "surinamese cartels",
        "moroccan drug barons",
        "moroccan drug traffickers",
        "northern moroccan hash kingpins",
        "southern moroccan traffickers",
        "nicaraguan drug dealers",
        "nicaraguan drug traffickers",
        "vietnamese drug trafficking organisations",
        "southeast asian organized crime networks",
        "turkish groups",
        "post-soviet organized crime groups",
        "italian organized crime groups",
        "italian crime syndicates",
        "lebanese organized crime groups",
        "nigerian organized crime groups",
        "israeli crime figures",
        "american mafia crime families",
        "new york families",
        "chicago families",
        "camorra gangs",
        "chechen mafia gangs",
        "bouncer mafia",
        "timber mafia",
        "timber mafia from assam",
        "timber mafia in brazil",
        "timber mafia in cambodia",
        "timber mafia in congo",
        "timber mafia in india",
        "timber mafia in indonesia",
        "timber mafia in romania",
        "tobacco mafia",
        "human smuggling organizations",
        "south african crime lords",
        "section organizations",
        "outlaw motorcycle gangs",
        "immigrant gangs",
        "immigrant drug gangs",
        "hispanic gangs",
        "latino gangs",
        "right-wing death squads",
        "paramilitary groups",
        "neo-integralist groups",
        "ahbash",
        "gerindo",
        "zaynabiyat",
        "belle starr gang",                  # defunct
        "bob dozier gang",                   # defunct
        "cochise cowboy gang",               # defunct
        "innocents gang",                    # defunct
        "jim miller gang",                   # defunct
        "soapy smith gang",                  # defunct
        "moltanee thugs",                    # defunct
        "soosea thugs",                      # defunct
        "telingana thugs",                   # defunct
        "thuggee",                           # defunct
        "thuggee gangs",                     # defunct
        "bradford gang",                     # defunct
        "grey mare lane gang",               # defunct
        "holland street gang",               # defunct
        "hope street gang",                  # defunct
        "meadow lads",                       # defunct
        "miles platting gang",               # defunct
        "scuttlers",                         # defunct
        "sloggers",                          # defunct
        "bhabani pathak gang",               # defunct
        "ghee hin",                          # defunct
        "hai san secret society",            # defunct
        "honghuzi",                          # defunct
        "hung society",                      # defunct
        "eastman gang",                      # defunct
        "jewish eastmans",                   # defunct
        "lenox avenue gang",                 # defunct
        "looney gang",                       # defunct
        "william gabel's gang",              # defunct
        "schuylkill rangers",                # defunct
        "provenzano family",                 # defunct
        "hudson dusters",                    # defunct
        "gopher gang",                       # defunct
        "four brothers",                     # defunct
        "lomonte gang",                      # defunct
        "navy street gang",                  # defunct
        "johnny spanish's gang",             # defunct
        "jacob orgen's gang",                # defunct
        "gustin gang",                       # defunct
        "blinky morgan gang",                # defunct
        "cheyenne gang",                     # defunct
        "mccart street gang",                # defunct
        "neapolitan camorristi",             # defunct
        "sicilian mafiosi",                  # defunct
        "neapolitan clans",                  # defunct
        "sicilian clans",                    # defunct
        "sicilian mafia faction (newark)",   # defunct
        "sicilian faction (newark)", #defunct
        "buccellato clan",                   # defunct
        "ciaculli faction",                  # defunct
        "croceverde giardini faction",       # defunct
        "green gang",                        # defunct
        "detroit crime family",              # defunct
        "villabate mafia family",            # defunct
        "mala del brenta",                   # defunct
        "gang des postiches",                # defunct
        "sige-sige gang",                    # defunct
        "callejeros",                        # defunct
        "latin souls",                       # defunct
        "koose munusamy veerappan gang",     # defunct
        "fratuzzi",          # defunct
        "megpunna thugs",    # defunct
        "sabini family",     # defunct
        "sindouse thugs",    # defunct
        "pissed off bastards motorcycle club",  # defunct
        "hell bent for glory biker club",       # defunct
        "market street commandos",              # defunct
        "redlands road runners",                # defunct
        "humber valley riders motorcycle club", # defunct
        "all england chapter",                  # defunct
        "throttle twisters",                    # defunct
        "phantom riders",                       # defunct
        "canadian lancers",                     # defunct
        "golden hawk riders",                   # defunct
        "animals",                              # defunct
        "straight satans motorcycle club",      # defunct
        "devil's disciples motorcycle club (boston)",  # defunct
        "queensmen motorcycle club",            # defunct
        "eastside mob",                         # defunct
        "westside mob",                         # defunct
        "pascuzzi combine",                     # defunct
        "manzella group",                       # defunct
        "sicilian mob",                         # defunct
        "d'aquila family",                      # defunct
        "d'aquila gang",                        # defunct
        "badami family",                        # defunct
        "tom gagliano's family",                # defunct
        "torrio-yale organization",             # defunct
        "porrello crime family",                # defunct
        "tagliavia family",                     # defunct
        "broadway mob",                         # defunct
        "city hall gang",                       # defunct
        "italian mob",                          # defunct
        "jewish gangster faction (newark)",     # defunct
        "third ward gang",                      # defunct
        "waxey gordon's gang",                  # defunct
        "santo trafficante sr.'s gang",         # defunct
        "the little jewish navy",               # defunct
        "forty-two gang",                       # defunct
        "thomas licavoli's gang",               # defunct
        "volpe brothers",                       # defunct
        "maceo organization",                   # defunct
        "maceo syndicate", #defunct
        "society for common progress",          # defunct
        "gang des tractions avant",             # defunct
        "tractions avant gang",                 # defunct (duplicate of above)
        "manson family",                        # defunct
        "charlestown mob",                      # defunct
        "mclaughlin gang",                      # defunct
        "joseph barboza's gang",                # defunct
        "bluebird gang",                        # defunct
        "blass gang",                           # defunct
        "kilbane brothers",                     # defunct
        "state line mob",                       # defunct
        "quality street gang",                  # defunct
        "nash gang",                            # defunct
        "freddie foreman's gang",               # defunct
        "richardson gang",                      # defunct
        "watney streeters",                     # defunct
        "zampa gang",                           # defunct
        "the corporation",                      # defunct
        "los halcones",                         # defunct
        "acquasanta mafia clan",                # defunct
        "mammoliti 'ndrina",                    # defunct
        "grecos",                               # defunct
        "la barbera brothers",                  # defunct
        "la barberas",                          # defunct
        "new mafia",                            # defunct
        "passo di rigano mafia family",         # defunct
        "francisci clan",                       # defunct
        "orsini clan",                          # defunct
        "venturi clan",                         # defunct
        "le milieu",                            # defunct
        "gallo brothers",                       # defunct
        "bill bonanno faction",                 # defunct
        "garfield boys",                        # defunct
        "knickerbocker avenue crew",            # defunct
        "vario crew",                           # defunct
        "north jersey faction",                 # defunct
        "disciple alliance",                    # defunct
        "gangster nation",                      # defunct
        "belairs",                              # defunct
        "drakers",                              # defunct
        "harrison gents",                       # defunct
        "bé bún's gang",                        # defunct
        "huỳnh tỳ's gang",                      # defunct
        "tài chém's gang",                      # defunct
        "ngô văn cái's gang",                   # defunct
        "chong-ro",                             # defunct
        "myung-dong",                           # defunct
        "chung ching yee",                      # defunct
        "white eagles",                         # defunct
        "pek kim leng",                         # defunct
        "slausons",                             # defunct
        "gladiators",                           # defunct
        "garfield place boys",                  # defunct
        "sackett street boys",                  # defunct
        "kane st. midgets",                     # defunct
        "degraw street boys",                   # defunct
        "butler gents",                         # defunct
        "south brooklyn angels",                # defunct
        "south brooklyn devils",                # defunct
        "south brooklyn diapers",               # defunct
        "the wanderers",                        # defunct
        "the little gents",                     # defunct
        "young savages",                        # defunct
        "gowanus boys",                         # defunct
        "the apaches",                          # defunct
        "the chaplains",                        # defunct
        "the jokers",                           # defunct
        "the untouchable bishops",              # defunct
        "maisto clan",                          # defunct
        "suki",                                 # defunct
        "nuova grande camorra pugliese",  # defunct
        "maceo organization",             # defunct
        "supreme gangsters",              # defunct
        "midgets",                        # defunct
        "savage seven",                   # defunct
        "spanish growth and development",       # defunct
        "mandingo warriors",                    # defunct
        "texas mafia",                          # defunct
        "ninevites",                         # defunct
        "charles birger gang",            # defunct
        "cuckoo gang",                    # defunct
        "cuckoos gang", #defunct
        "down river gang",                # defunct
        "downriver gang", #defunct
        "hogan gang",                     # defunct
        "lonardo brothers faction",       # defunct
        "minneapolis combination",        # defunct
        "minneapolis syndicate",          # defunct
        "pendergast machine",             # defunct
        "philadelphia faction",           # defunct
        "ragen's colts",                  # defunct
        "russo gang",                     # defunct
        "shelton brothers gang",          # defunct
        "shelton gang",                   # defunct
        "southside o'donnell brothers",   # defunct
        "terminal gang",                  # defunct
        "valley gang",                    # defunct
        "westside o'donnell's",           # defunct
        "zwillman gang",                  # defunct
        "schirò family",                  # defunct
        "young turks",                    # defunct
        "garuda putih",                   # defunct
        "pasukan siluman",                # defunct
        "pemotong leher",                 # defunct
        "srigala hitam",                  # defunct
        "brigada blanca",                 # defunct
        "mandamas mc",                    # defunct
        "the creativity movement" ,
        "islamic courts union", #defunct
        "islamic court union", #defunct
        "mujahideen commanders",                   # generic
        "new illegal armed groups",                # generic
        "pindari bands",                           # historical generic
        "pindaris",                                # historical generic (duplicate of above)
        "police death squad",                      # generic
        "regional militia commanders",             # generic
        "right-wing paramilitary groups",          # generic
        "mujahideen commanders",                   # generic
        "new illegal armed groups",                # generic
        "pindari bands",                           # historical generic
        "pindaris",                                # historical generic (duplicate of above)
        "police death squad",                      # generic
        "regional militia commanders",             # generic
        "right-wing paramilitary groups",          # generic
        "abbotsford east asian crime groups",
        "abbotsford multicultural street gangs",
        "aboriginal street gangs",
        "afghan street gangs",
        "american motorcycle gangs",
        "baby gangs",
        "brantford biker and street gangs",
        "counter-revolutionary gangs",
        "dutch antillean criminal organizations",
        "east african street gangs",
        "ethiopian drug trafficking groups",
        "gangs of jamaican canadians",
        "greek and italian greaser gangs",
        "hispanic and latino gangs in the folk nation alliance",
        "hispanic prison and street gangs",
        "indian-origin crime groups in canada and uk",
        "italian immigrant criminal gangs",
        "italian-american gangs",
        "italian-american gangsters",
        "karachi-based gangs",
        "london street gangs",
        "mexican street gangs",
        "middle-eastern gangs",
        "montreal haitian street gangs",
        "moroccan street gangs of antwerp and amsterdam",
        "multicultural street gangs",
        "new york-based drug gangs",
        "nigerian drug gangs",
        "nigerian pirate gangs",
        "other costa mesa gangs",
        "other gangs from hanoi, namdinh, and haiphong",
        "pakistani street gangs",
        "pirate gangs",
        "punjabi street gangs",
        "romanian groups",
        "samoan criminal outfits",
        "socialist revolutionary gangs",
        "somali drug trafficking groups",
        "somali pirate gangs",
        "south asian gangs",
        "south sudanese gangs",
        "speke-based drug gang",
        "sudanese drug trafficking groups",
        "surrey south asian street gangs",
        "surrey south-east asian street gangs",
        "tamil street gangs",
        "teenage criminal gang",
        "west indian (caribbean) street gangs",
        "white prison and street gangs",
        "triad", # generic term
        "kosovar-albanian crime groups",           # clan - generic
        "jewish crime syndicates",                 # faction - generic
        "nigerian crime syndicates",               # faction - generic
        "hamilton-based mafia families",            # mafia - generic
        "italian-american crime syndicates",        # mafia - generic
        "montreal-based organized crime group",     # mafia - generic
        "palermo-based mafia families of stefano bontade and salvatore inzerillo",  # mafia - generic
        "macanese organized crime groups",          # triad - generic
        "hải phòng criminal underworld",           # faction - generic
        "harlem underworld",                       # faction - generic
        "montreal underworld",                     # faction - generic
        "mumbai underworld",                       # faction - generic
        "kachin conflict factions",                # faction - generic
        "iraqi group",                             # faction - too vague
        "syrian rebels",                           # faction - generic
        "cali drug-trafficking groups",        # cartel — generic
        "medellín drug-trafficking groups",    # cartel — generic
        "goan drug trade groups",              # cartel — generic
        "colombian crime cartel",              # cartel — generic
        "movement for self-determination",     # Corsican nationalist political movement
        "islamic courts union",                # Somali governing body (anti-piracy operations), not a criminal org
        "dublin-based chinese gang",                       # unnamed gang, one-off reference
        "fujianese organized crime",                       # umbrella term, not a specific org
        "israeli organized crime",                         # umbrella term
        "middle eastern/lebanese organized crime gangs",   # umbrella term
        "native hawaiian crime syndicates",                # umbrella term
        "serbian organized crime group",                   # unnamed, one-off reference
        "armenian organized crime group",                  # unnamed, one-off reference
        "antonio bardellino",     # "A Camorra boss from San Cipriano d'Aversa" — person, not org
        "michele greco",          # "A Mafia boss controlling the Naples family" — person, not org
        "michele zaza",           # "A Camorra boss with strong ties to Cosa Nostra" — person, not org
        "le milieu",                                # description: "A general term for native historical French criminal organizations"
        "melbourne gangland killings participants",  # description: "A collective term for various criminal factions and individuals"
        "16/12s",  # Defunct faction (1990s-1998) with no known successor
        "17th street gang",  # Defunct subset/clique with no known successor, no historical significance, and zero connections.
        "17th street locas",  # Defunct former clique with no known successor, no historical significance, and zero connections.
        "17th street tiny locos",  # Defunct former clique with no known successor, no historical significance, and zero connections.
        "20th streeters",  # Defunct former clique with no known successor, no historical significance, and zero connections.
        "21st. deadend winos",  # Defunct former clique with no known successor, no historical significance, and zero connections.
        "70-side",  # Defunct Wah Ching branch with no known successor, no historical significance, and zero connections.
        "abate clan",  # Defunct mafia clan with no known successor or modern relevance
        "aces and eights motorcycle club",  # Defunct motorcycle club patched over in 1983, no known successor, and zero connections.
        "alianza fronteriza",  # Defunct cartel with no known successor or modern relevance.
        "aliens motorcycle gang",  # Defunct club patched over by Hells Angels, not a separate entity.
        "all maravilla sets",  # Defunct gang collective with no known successor or modern relevance.
        "alperon crime family",  # Effectively ended in 2008 with no known successor or ongoing influence.
        "angelo heights sureños",  # Defunct gang with no known successor or modern relevance
        "annihilators motorcycle club",  # Defunct Canadian biker club from 1980s-1990s with no clear modern successor.
        "anthony perish criminal gang network",  # Defunct gang network with no known successor or modern relevance.
        "apex",  # Defunct hooligan firm amalgamated into Zulu Warriors in 1982, no independent existence.
        "apocalypse riders",  # Defunct motorcycle club with no known successor or modern relevance.
        "armata corsa",  # Defunct militia with no known successor or modern relevance.
        "aryan republican army",  # Defunct since 1996, no successor, limited historical impact on modern organized crime.
        "asap",  # Defunct gang with no known successor or modern relevance.
        "ashmont",  # Defunct gang with no known successor or modern relevance.
        "assassins",  # Proposed name for Crips, never an actual organization.
        "aston villa hooligan firms",  # Defunct hooligan group with no known successor or modern relevance.
        "atlantic city skinheads",  # Defunct group with no known successor or modern relevance
        "atomes mc",  # Defunct since 1984 with no known successor or modern relevance
        "atx",  # Defunct gang with no known successor or modern relevance
        "ba dương's bình xuyên",  # Defunct historical faction with no successor or modern relevance.
        "baby regulators",  # Defunct gang with no known successor or modern relevance.
        "bad 7 motorcycle club",  # Defunct motorcycle club with no known successor or modern relevance.
        "badalamenti mafia clan",  # Defunct Mafia clan with no known successor or modern relevance.
        "badami newark family",  # Defunct 1930s mafia family disbanded by the Commission, no successor, and no modern relevance.
        "baghdad crew",  # Defunct gang with no known successor or modern relevance.
        "balistrieri family",  # Defunct Mafia family with no known successor or modern relevance.
        "banana gang",  # Dismantled in 2015, no clear successor or ongoing relevance.
        "banda e durrësit",  # Defunct since 2005 with no known successor or lasting impact on global organized crime.
        "banda e lushnjës",  # Defunct since 2006 with no known successor or significant legacy in modern criminal networks.
        "bande des trois canards",  # Defunct 1950s-60s gang with no successor or modern relevance.
        "barbarians",  # Local motorcycle gang with no clear ongoing activity or broader significance.
        "barbudos faction",  # Defunct faction with no known successor or modern relevance
        "barhoppers motorcycle club",  # Minor club with no clear ongoing activity or broader significance.
        "baron criminal group",  # Local group with no clear ongoing activity or broader significance.
        "barrow gang",  # Defunct 1932-1934 gang with no successor, no modern relevance, and only 1 connection.
        "batallón vasco español",  # Defunct death squad (1975-1987) with no successor or modern criminal relevance
        "belanoca gang",  # Local loan shark group with no clear ongoing activity or broader significance.
        "beneduce-longobardi clan",  # Historical Camorra clan with no clear ongoing activity or successor.
        "bin laden records",  # Local gang division with no clear ongoing activity or broader significance.
        "birra clan",  # Defunct clan with no known successor or modern relevance
        "bizzarro clan",  # Defunct clan with no known successor or modern relevance
        "black angels mc",  # Defunct local motorcycle club with no known successor or modern relevance
        "black b. inc.",  # Front organization with no clear ongoing activity or broader significance.
        "black ghost",  # Defunct gang with no known successor or modern relevance
        "black overlords",  # Proposed name for Crips, never an actual organization.
        "black rhinos mc",  # Support club with no clear ongoing activity or broader significance.
        "blagardsplads",  # Immigrant gang with no clear ongoing activity or broader significance.
        "blonks",  # Bikie gang patched over to Hells Angels in 1993, no longer independent
        "bloods & crips",  # Gangsta rap group, not a primary criminal organization.
        "bloody devils",  # Defunct biker gang patched over to Hells Angels in 1973, no independent legacy.
        "bogota cartel",  # Defunct cartel with no known successor or modern relevance, historically minor.
        "bonanno faction (1964-1968)",  # Defunct internal faction with no successor as an independent entity, only 1 connection.
        "booze runners motorcycle club",  # Absorbed by Outlaws MC in 1991, no independent existence
        "bordonaro family",  # Defunct crime family with no known successor or modern relevance.
        "boston mafia family (pre-1932)",  # Defunct pre-Prohibition-1932 group with no clear successor to that specific entity, only 1 connection.
        "boyd gang",  # Defunct historical gang with no successor or modern relevance.
        "bridge street gang",  # Fictional organization from TV series Oz, not a real criminal group.
        "brodie gang",  # Fictional organization from video game Mafia II, not a real criminal group.
        "bulgarian cocaine trafficking group",  # Defunct since 2008 with no known successor or lasting legacy in global crime.
        "bullshit mc",  # Defunct since 1988 with no known successor or modern relevance
        "bảy viễn's bình xuyên",  # Defunct historical faction with no successor or modern relevance.
        "calliope porch boyz",  # Reportedly eliminated by 2003, no known successor or modern relevance.
        "campbell boys",  # Defunct gang from 1989 with no legacy or modern relevance
        "campbell brothers",  # Defunct gang with no known successor or modern relevance.
        "carbone-spirito clan",  # Defunct Corsican clan dissolved by 1943, no clear successor, and limited historical relevance to modern global crime.
        "cardiff city hooligans",  # Defunct hooligan group with no known successor or modern relevance.
        "castellammarese mafia",  # Defunct 1930-1931 group with no successor, historically significant but only 1 connection and not a direct modern lineage.
        "castle square cubs",  # Defunct local gang with no known successor or modern relevance.
        "caveira",  # Defunct Brazilian faction with no known successor or modern relevance
        "central park gang",  # Defunct local gang with no known successor or modern relevance.
        "charlestown gang",  # Defunct Irish-American gang from 1950s-1966 with no known successor or modern relevance.
        "cherokees",  # Consolidated into Vice Lord Nation in 1970s, no independent existence
        "clan bardellino",  # Historical Camorra clan with no known active successor or modern relevance.
        "clovers",  # Consolidated into Vice Lord Nation in 1970s, no independent existence
        "club de clichy",  # Patched over by Bandidos in 1989, no longer exists as independent entity
        "cobras",  # Consolidated into Vice Lord Nation in 1970s, no independent existence
        "coll crew",  # Defunct 1930-1932 gang with no successor, no modern relevance, and only 1 connection.
        "commanches",  # Defunct Chicago gang consolidated into Vice Lord Nation in 1970s, no known successor or modern relevance.
        "continental pimps",  # Defunct Chicago gang consolidated into Vice Lord Nation in 1970s, no known successor or modern relevance.
        "continentals",  # Defunct early 1960s gang with no successor or modern relevance.
        "corso dei mille family",  # Defunct Sicilian Mafia family with no known successor or modern relevance
        "cosoleto crime family",  # Fictional 'Ndrangheta family from TV series Bad Blood, not a real criminal organization
        "cozzilino clan",  # Defunct Camorra clan with no known successor or modern relevance
        "cult biker club",  # Defunct biker club that merged with the Outlaws MC in 1964, no distinct modern existence or relevance.
        "cămătarilor clan",  # Defunct Romanian clan from 1990s with no known successor or modern relevance
        "da nang boys",  # Fictional gang from a video game (Grand Theft Auto: San Andreas), not a real-world criminal organization.
        "dalton gang",  # Defunct 19th century gang with no successor or modern relevance
        "dalton network",  # Defunct with no known successor or modern relevance
        "damnés",  # Defunct biker gang patched over to Hells Angels in 1987, no independent legacy.
        "danny greene's gang",  # Defunct 1976-1977 gang with no successor, no modern relevance, and only 1 connection.
        "daulat ul-islamiya",  # Defunct with no known successor or modern relevance
        "de micco clan",  # Defunct with no known successor or modern relevance
        "dead men",  # Defunct with no known successor or modern relevance
        "death before dishonour",  # Defunct with no known successor or modern relevance
        "denver crime family",  # Defunct with no known successor or modern relevance
        "desperados motorcycle club",  # Defunct with no known successor or modern relevance
        "dev-sol",  # Defunct with no known successor or modern relevance
        "devil's disciples motorcycle club (canada)",  # Defunct Canadian motorcycle club (1965-1976) with no known successor or modern relevance.
        "dhure group",  # Defunct with no known successor or modern relevance
        "di maggio clan",  # Defunct with no known successor or modern relevance
        "diablos lobos",  # Defunct with no known successor or modern relevance
        "dirty knuckles mc",  # Defunct with no known successor or modern relevance
        "dirty reich mc",  # Defunct club rebranded, no independent modern presence.
        "district 61",  # Defunct with no known successor or modern relevance
        "dogpack 416",  # Defunct with no known successor or modern relevance
        "dogs of hell",  # Fictional organization from Daredevil, not a real criminal group
        "dominican power",  # Defunct with no known successor or modern relevance
        "domnu clan",  # Defunct with no known successor or modern relevance
        "dorchester ave mavericks",  # Defunct with no known successor or modern relevance
        "downtown gang",  # Defunct Prohibition-era gang in Galveston with no known successor or modern relevance.
        "dragoons mc",  # Defunct with no known successor or modern relevance
        "draper street",  # Defunct with no known successor or modern relevance
        "dubai mafia",  # Defunct with no known successor or modern relevance
        "dudley park",  # Defunct local gang with no known successor or modern relevance
        "dudley street park",  # Defunct local gang with no known successor or modern relevance
        "duroc",  # Defunct local gang with no known successor or modern relevance
        "earth angelettes",  # Defunct female gang disbanded in 1990, no successor, minimal relevance to current global crime landscape.
        "earth angels",  # Defunct sub-clique disbanded in 1990 with no successor
        "east boston gang",  # Defunct gang associated with historical figure Joseph Barboza, no known successor or current relevance.
        "east ferry street gang",  # Defunct local gang with no known successor or modern relevance
        "east london chapter",  # Defunct early Hells Angels chapter, absorbed into broader Hells Angels organization.
        "east side 13",  # Defunct local gang with no known successor or modern relevance
        "east side forming kaos",  # Absorbed circa 2000, no independent existence or successor.
        "easton crew",  # Defunct crew that patched over in 2011, no independent existence or current relevance.
        "eastside gang",  # Defunct 1920s-1931 faction with no successor, no modern relevance, and only 1 connection.
        "eastside ontario 4th street ygw",  # Defunct local gang with no known successor or modern relevance
        "elizabeth mafia faction",  # Defunct historical faction with no clear successor or modern relevance.
        "elmwood st 13",  # Defunct local gang with no known successor or modern relevance
        "escape hell mc",  # Defunct motorcycle club with no known successor or modern relevance
        "everton geneva grizzleys",  # Defunct local gang with no known successor or modern relevance
        "executeurs",  # Defunct group whose members joined Rock Machine, no independent existence or current relevance.
        "familia stones",  # Defunct local gang with no known successor or modern relevance
        "filthy rebels",  # Defunct Hells Angels-affiliated gang from 1991 with no legacy
        "fischer fools",  # Defunct local gang with no known successor or modern relevance
        "fujita-gumi",  # Defunct gang whose remnants merged into Matsuba-kai in 1953, no clear modern successor or relevance to current criminal landscape.
        "fullerton boys",  # Local gang with no evidence of continued activity or broader relevance.
        "gali",  # Defunct state-backed groups from Suharto era, no independent criminal legacy.
        "gang groups supporting captain triệu",  # Obscure defunct groups with no known successor or modern relevance.
        "garduña",  # Defunct mythical organization with no verified historical existence or modern legacy.
        "ghee hin kongsi",  # Defunct 19th-century secret society with no clear modern successor or relevance to current global crime landscape.
        "ghee hin society",  # Defunct historical secret society with no clear modern successor or relevance to current organized crime landscape.
        "ghost gang",  # Local gang with no evidence of current activity or broader significance beyond historical rivalry.
        "gitans moto club",  # Defunct French-Canadian motorcycle club from 1967-1984 with no clear modern successor.
        "goonie gang",  # Local gang with no evidence of current activity or broader significance beyond historical rivalry.
        "gravina crime family",  # Fictional organization from Mafia II video game, not a real criminal group.
        "green ones",  # Defunct Sicilian gang from 1915-1920s with no known successor or modern relevance.
        "greenwood street posse",  # Local gang with no evidence of current activity or broader significance beyond historical rivalry.
        "grupos antiterroristas de liberación",  # Defunct death squad (1975-1987) with no successor or modern criminal relevance
        "gypsy outlaws",  # Defunct gang absorbed by Outlaws MC, not a separate entity.
        "hackers biker gang",  # Defunct club patched over by Hells Angels, not a separate entity.
        "heath st",  # Local gang with no evidence of current activity or broader significance beyond historical rivalry.
        "hells angels vikings",  # Defunct 1969-1975 group that renamed/evolved; not a direct predecessor of modern Hells Angels, only 1 connection.
        "hells de crimée",  # Defunct biker gang patched over to Hells Angels in 1981, no independent legacy.
        "henchmen mc",  # Motorcycle club patched over to Outlaws in 1993, no longer independent
        "high spirits motorcycle club",  # Defunct since 1994 amalgamation, no successor, low historical significance.
        "hole in the wall gang",  # Defunct 1971-1980s crew with no successor, no modern relevance, and only 1 connection.
        "holocaust motorcycle club",  # Defunct since 1988 with no known successor or modern relevance
        "hop sing boys",  # Defunct gang dismantled by police after 1977 massacre, no known successor or modern relevance.
        "hpl 45s",  # Defunct faction (1990s-1998) with no known successor
        "hùng cốm's gang",  # Defunct gang active only 1985-1990 with no modern legacy
        "ignacio antinori's organization",  # Defunct early 20th-century mafia organization with no successor, no modern relevance, and no major historical impact.
        "ikeshita-gumi",  # Fictional yakuza syndicate from video game (Tom Clancy's Rainbow Six), not a real criminal organization.
        "imperial japanese yakuza",  # Defunct colonial-era group with no direct successor operating today, and limited relevance to modern yakuza.
        "imperials",  # Defunct historical group that merged into Latin Kings, not a separate criminal entity.
        "iron coffins",  # Defunct gang from 1985 with no known successor or modern relevance.
        "iron cross gang",  # Defunct gang patched over to Outlaws MC in 1970, no successor or modern relevance.
        "israeli mafia in new york",  # Disbanded after 1990 with no successor organization
        "james gang mc",  # Defunct motorcycle club from 1992 with no known successor or modern relevance.
        "james-younger gang",  # Defunct 19th-century outlaw gang with no successor or relevance to modern organized crime.
        "jr earth angels",  # Defunct sub-clique of OVS, disbanded with no known successor or modern relevance.
        "jso",  # Dissolved in 2003 with no known successor or legacy relevant to modern organized crime.
        "jumok",  # Defunct colonial-era Korean street gang with no successor, no modern relevance, and no historical significance.
        "kagotora-gumi",  # Defunct historical yakuza clan with no known active successor or modern relevance.
        "kanto hatsuka-kai",  # Defunct bakuto federation with no known active successor or modern relevance.
        "keiji union",  # Defunct yakuza splinter group with no known active successor or modern relevance.
        "kid dropper's gang",  # Defunct early 20th century labor slugging gang, no modern successor or relevance.
        "kitchen irish",  # Fictional gang from Daredevil, not a real criminal organization.
        "kloekhorststraat gang",  # Local Amsterdam gang with no known broader significance or modern relevance.
        "koo majok's mob",  # Defunct colonial-era Korean mafia with no successor, no modern relevance, and no historical significance.
        "kop of boulogne",  # Defunct hooligan firm with no known successor or modern relevance.
        "krazy getdown boys",  # Defunct gang with no known successor or modern relevance.
        "krieger verwandt",  # Defunct gang with no known successor or modern relevance.
        "kuntsevskaya gang",  # Defunct gang with no known successor or modern relevance.
        "kyoyu-kai",  # Defunct yakuza group with no successor and no connections in network
        "la conexión",  # Defunct cartel with no known successor or modern relevance.
        "la mayiza",  # Defunct cartel with no known successor or modern relevance.
        "la nueva empresa",  # Defunct cartel with no known successor or modern relevance.
        "lal gang",  # Existed only in 2007 with no successor or lasting impact on organized crime.
        "lazy gents",  # Defunct gang with no known successor or modern relevance.
        "lemonwood chiques",  # Defunct gang with no known successor or modern relevance.
        "lennox 13",  # Defunct gang with no known successor or modern relevance.
        "lennox 13 park village compton crips",  # Defunct gang with no known successor or modern relevance.
        "lepakko gang",  # Defunct gang with no known successor or modern relevance.
        "little locos",  # Minor clique with no connections and unclear historical significance
        "little village gaylords",  # Defunct gang, unrelated to active organizations, no modern relevance.
        "liverpool fc hooligans",  # Defunct hooligan firm with no known successor or modern relevance.
        "loh kuan",  # Defunct local gang in Singapore with no known successor or modern relevance
        "lomas",  # Defunct gang with no known successor or modern relevance
        "los anormales",  # Local gang in Puerto Rico with no known successor or modern relevance
        "los desmadrosos",  # Clique within Forming Kaos with no known successor or modern relevance
        "los extranjeros",  # Local gang in Puerto Rico with no known successor or modern relevance
        "los kilos",  # Defunct gang with no known successor or modern relevance
        "los malcriados 3ad",  # Defunct gang with no known successor or modern relevance
        "los originales",  # Clique within Forming Kaos with no known successor or modern relevance
        "los tanzanios",  # Defunct gang with no known successor or modern relevance
        "los tlacos",  # Defunct gang with no known successor or modern relevance
        "lvm gang",  # Defunct local gang in California with no known successor or modern relevance
        "m16",  # Defunct gang with no known successor or modern relevance
        "m58 firm",  # Sub-division of the Red Army with no known successor or modern relevance
        "machos",  # Defunct private army of a dissolved cartel, no modern successor
        "mad swan bloods",  # Defunct gang with no known successor or modern relevance
        "madhiban with attitude",  # Defunct gang with no known successor or modern relevance
        "mafia families of hamilton",  # Defunct local mafia families in Canada with no known successor or modern relevance
        "maggapa",  # Defunct 1996 copycat gang with no lasting legacy or successor.
        "malhi-buttar coalition",  # Defunct coalition with no known successor or relevance to current global organized crime.
        "mamak gang",  # Defunct since 2006 with no known successor or relevance to current global organized crime.
        "maranzano faction",  # Defunct 1920s-1931 faction with no successor, historically significant but only 1 connection and not a direct modern lineage.
        "martha organization",  # Defunct organization with no known successor or relevance to current global organized crime.
        "masaki-gumi",  # Disbanded yakuza group with no known successor or modern relevance.
        "mascot street killas",  # Defunct street gang with no known successor or relevance to current global organized crime.
        "matranga family",  # Defunct early 20th-century mafia family with no successor, no modern relevance, and no major historical impact on organized crime.
        "mau maus",  # Defunct gang with no known successor or relevance to current global organized crime.
        "maximus ocg",  # Defunct since 2009 with no known successor or ongoing impact on global criminal networks.
        "mcsween gang",  # Defunct historical gang with no successor or modern relevance.
        "menard brotherhood",  # Defunct prison gang with no known successor or relevance to current global organized crime.
        "mercado do povo atitude",  # Defunct faction with no known successor or relevance to current global organized crime.
        "merciless riders",  # Defunct motorcycle club with no known active legacy or modern significance.
        "merciless souls motorcycle club",  # Defunct motorcycle club with no known successor or relevance to current global organized crime.
        "mexican team work",  # Defunct youth branch with no known successor or relevance to current global organized crime.
        "midwest drifters",  # Defunct motorcycle club absorbed by another group, no active legacy or modern relevance.
        "milla gangsta bloods",  # Defunct gang with no known successor or relevance to current global organized crime.
        "millwall bushwackers",  # Defunct hooligan firm with no known successor or relevance to current global organized crime.
        "mineo gang",  # Defunct early 20th-century gang absorbed by another organization, no independent successor, and no lasting historical significance.
        "mo dumplings gang of british columbia",  # Obscure gang with no known activity or modern relevance.
        "mobshitters",  # Defunct gang with no known successor or relevance to current global organized crime.
        "morphines",  # Defunct 1970s gang absorbed into Vice Lord Nation, no modern successor or relevance.
        "moston rats",  # Defunct sub-division of Red Army with no modern successor or relevance.
        "motion lounge crew",  # Defunct crew of Bonanno family with no modern successor or relevance.
        "muchi-konkai",  # Fictional gang from manga Nisekoi, not a real-world criminal organization
        "mullen gang",  # Defunct c. 1950s–1972 gang with no successor, no modern relevance, and only 1 connection.
        "mulner organization",  # Defunct with no details, no modern successor or relevance.
        "multigroup",  # Defunct since 2003 with no clear successor or ongoing relevance to current global criminal networks.
        "nakanishi-gumi",  # Defunct faction from Yama-Ichi War era, no modern successor or relevance.
        "new york city mafia (masseria)",  # Defunct 1930-1931 group with no successor, historically significant but only 1 connection and not a direct modern lineage.
        "newark family",  # Defunct 1930s mafia family with no successor, no modern relevance, and no major historical impact.
        "newton gang",  # Defunct 1919-1924 gang with no successor, no modern relevance, and only 1 connection.
        "night sinners",  # Defunct biker gang absorbed by Outlaws with no independent existence
        "nishida-gumi",  # Defunct with leader killed in 2003, no modern successor or relevance.
        "no surrender crew canada",  # Dissolved after 2006 with no known successor or ongoing influence on global crime.
        "noe-schultz gang",  # Defunct Prohibition-era gang absorbed by others, no direct successor, limited historical significance beyond local Bronx operations.
        "oakdale mob",  # Defunct gang with no modern successor or relevance.
        "outcasts motorcycle club",  # Defunct since 1970 with no known successor or modern relevance.
        "palmers mc",  # Defunct support club for Rock Machine, no known successor or modern relevance.
        "para-dice riders motorcycle club",  # Defunct after patching over to Hells Angels in 2000, no known successor or current relevance.
        "patriotic revolutionary youth movement",  # Defunct youth wing of PKK, succeeded by Civil Protections Units, no current independent existence.
        "pharaohs motorcycle club",  # Defunct motorcycle club with no known successor or modern relevance, only historical territorial mention.
        "philip paul's gang",  # Defunct early 20th-century gang from a brief conflict, no lasting impact or modern relevance.
        "philtown",  # Defunct motorcycle club involved in past power struggles, no known successor or current relevance.
        "pillow gang",  # Defunct early Italian gang from 1910-1930s with no clear successor or modern relevance.
        "polish counts",  # Defunct gang from 1959 with no known successor or modern relevance.
        "porrello brothers faction",  # Defunct 1926-1930 faction with no successor, no modern relevance, and only 1 connection.
        "providence mafia family",  # Defunct early 20th-century mafia family with no successor, no modern relevance, and no major historical impact.
        "pyrates",  # Defunct university confraternity with no clear successor or modern relevance
        "raving mad brothers",  # Absorbed in 1999, no independent existence or successor.
        "rise above movement",  # Defunct faction with no clear successor or modern relevance
        "river gang",  # Defunct Prohibition-era gang with no successor, no modern relevance, and only 1 connection.
        "river thugs",  # Defunct 19th-century gang with no successor or modern relevance.
        "road eagles",  # Defunct motorcycle club patched over by Bandidos, no known modern activity or legacy.
        "rosenzweig's gang",  # Defunct early 20th-century labor slugging gang with no legacy or modern relevance.
        "salem st cholos",  # Historical street clique with no indication of current activity or relevance to modern organized crime.
        "sam catalanotte gang",  # Defunct early 20th-century gang that was a predecessor but has no direct active successor or modern operational relevance.
        "san francisco crime family",  # Defunct by 2006 with no clear successor; limited modern relevance.
        "sarasota assassination society",  # Defunct late 19th-century faction with no successor, no modern relevance, and no historical significance to modern organized crime.
        "satan's syndicate",  # Defunct biker club that patched over in 2002, with no successor or modern relevance.
        "schultz gang",  # Defunct 1920s-1935 gang with no successor, no modern relevance, and only 1 connection.
        "shang kal",  # Defunct colonial-era Korean mob with no successor, no modern relevance, and no historical significance.
        "shin majak",  # Defunct colonial-era Korean mafia with no successor or modern relevance.
        "shuei-gumi",  # Fictional organization from manga, not a real criminal group
        "sinners motorcycle club",  # Disbanded after 1992 incident with no known successor
        "six-mile syndicate",  # Fictional gang from a novel, not a real criminal organization
        "slonovskaya gang",  # Defunct since 2000 with no known successor, minimal historical impact on current global crime landscape.
        "south london chapter",  # Defunct early Hells Angels chapter, absorbed into broader Hells Angels organization.
        "steamers",  # Defunct hooligan firm with no successor or relevance to modern organized crime
        "symbionese liberation army",  # Defunct since 1975 with no successor, historically a leftist militant group not shaping modern organized crime.
        "tashma-baz thugs",  # Defunct 19th-century gang with no successor, no modern relevance, and no historical significance to modern organized crime.
        "terrible josters",  # Fictional or defunct British gang with no evidence of real-world existence or modern relevance.
        "the fancy boys",  # Fictional or defunct British gang with no evidence of real-world existence or modern relevance.
        "the johnson gang",  # Defunct since 2006 with no known successor or relevance to modern global organized crime.
        "the terrible west siders",  # Fictional or defunct British gang with no evidence of real-world existence or modern relevance.
        "thirteenth tribe",  # Defunct biker gang patched over to Hells Angels in 1984, no independent legacy.
        "tokyo boys",  # Defunct since 1996 with no known successor or modern relevance
        "tren del llano",  # Mentioned as dismantled past gang in Venezuela, no indication of successor or current relevance.
        "tri-county mc",  # Defunct motorcycle club with only one historical incident in 1994, no known successor or modern relevance.
        "triple a",  # Defunct death squad (1975-1987) with no successor or modern criminal relevance
        "tucson crew",  # Defunct faction of the Bonanno crime family with no known successor or modern relevance.
        "turatello gang",  # Defunct since 1981 with no known successor or modern relevance.
        "unionen mc",  # Defunct motorcycle club that became Hells Angels chapter, no independent existence.
        "vallelunga pratameno mafia family",  # Defunct Sicilian Mafia family from 1978 with no known successor or modern relevance.
        "vietnamese flying dragons",  # Defunct branch of the Flying Dragons with no known successor or modern relevance.
        "vigilantes motorcycle club",  # Defunct support club absorbed by the Loners with no known successor or modern relevance.
        "wah sang society",  # Defunct society absorbed in 1854 with no known successor or modern relevance.
        "weather underground",  # Defunct left-wing terrorist organization from the 1970s with no successor or relevance to modern organized crime.
        "wheeling faction",  # Defunct since 2008 with no known successor or relevance to modern organized crime.
        "wheelmen motorcycle club",  # Absorbed by Outlaws in 1993, no longer independent
        "white hand gang",  # Defunct early 1900s Irish gang, disappeared by 1925 with no modern successor.
        "wild ones",  # Defunct motorcycle club from 1970s-1979, part of Satan's Choice alliance but no modern successor.
        "yao lai",  # Defunct 1969-1970s gang with no clear successor or modern relevance, only 1 connection.
        "yau lai", #defunct
        "bensonhurst crew", #defunct
        "youngstown faction",  # Defunct faction (1970s-1999) with no known successor
        "newark family (mafia)", #defunct
        "french connection", # defunct
        "french connection crew", #defunct
        "milwaukee crime family", #defunct
        "zwi migdal",  # Defunct historical organization (1860s-1939) with no modern successor or direct relevance to current criminal landscape.
        "cali cartel", # defunct,
        "colombian cali cartel", #defunct
        "alfieri clan", #defunct
        "sarno clan", #defunct
        "albanian Boys", #defunct
        "medellín cartel", #defunct
        "banda della magliana", #defunct
        "satan's angels", #defunct
        "satan's angels motorcycle club", #defunct
        "nøragersmindebanden", #defunct
        "ashland royals", #defunct
        "aryan syndicate", #defunct
        "monterey park asian boyz", #defunct
        "asian boys insanity", #defunct
        "van nuys asian boys", #defunct
        "beltrán-leyva cartel",                          # defunct
        "autodefensas unidas de colombia",               # defunct
        "united self-defense forces of colombia",  # defunct
        "united self-defense forces of colombia (auc)",  # defunct
        "united self-defense units of colombia",  # defunct
        "auc",                                           # defunct  
        "supreme team",                                  # defunct
        "joe boys",                                      # defunct
        "beltrán-leyva cartel",                          # defunct
        "supreme team",                                  # defunct
        "joe boys",                                      # defunct
        "east harlem purple gang",                       # defunct
        "grim reapers motorcycle club (canada)",         # defunct
        "clan marfella",                                 # defunct
        "bontade mafia family",                          # defunct
        "north side gang",                               # defunct
        "axe gang",                                      # defunct
        "azerbaijani grey wolves",                       # defunct
        "benvenuto clan",                                # defunct
        "bonanno faction",                               # defunct
        "born to kill",                                  # defunct
        "central warriors",                              # defunct
        "charles martel group",                          # defunct
        "chicago heights crew",                          # defunct
        "chijon family",                                 # defunct
        "colonna crime family",                          # defunct
        "fk gang",                                       # defunct
        "fob gang",                                      # defunct
        "hai san society",                               # defunct
        "islamic courts union",                          # defunct
        "khmer rouge",                                   # defunct
        "kurdistan freedom brigades",                    # defunct
        "kurdistan liberation force", #defunct
        "kurdistan people's liberation force",           # defunct
        "people's liberation army of kurdistan",         # defunct
        "los extraditables",                             # defunct
        "the extraditables", #defunct
        "magliulo clan",                                 # defunct
        "mcdonald ave crew",                             # defunct
        "melrose park crew",                             # defunct
        "metz gang",                                     # defunct
        "miami boys",                                    # defunct
        "morticians",                                    # defunct
        "national liberation front of kurdistan",        # defunct
        "new york city mafia",                           # defunct
        "north side crew",                               # defunct
        "organisation armée secrète",                    # defunct
        "peace mod",                                     # defunct
        "ruthless warriors",                             # defunct
        "sekine-gumi",                                   # defunct
        "sendero rojo",                                  # defunct
        "special organization",                          # defunct
        "taylor street crew",                            # defunct
        "the council",                                   # defunct
        "the family",                                    # defunct
        "united islamic front for the salvation of afghanistan",  # defunct
        "yao lai",                                       # defunct
        "yiddish black hand",                            # defunct
        "banda della comasina",                          # defunct
        "black hand",                                    # defunct
        "black hand organizations",                      # defunct
        "dead rabbits",                                  # defunct
        "egan's rats",                                   # defunct
        "five points gang",                              # defunct
        "genna brothers",                                # defunct
        "genna crime family",                            # defunct
        "purple gang",                                   # defunct
        "purple gang (detroit)",                         # defunct
        "the purple gang",                               # defunct
        "red army faction",                              # defunct
        "rote armee fraktion", #defunct
        "red brigades",                                  # defunct
        "whyos",                                         # defunct
        "de stefano-tegano-libri-latella clans",         # defunct
        "palermo centro mafia family",                   # defunct
        "masseria family",                               # defunct
        "masseria clan",  # defunct
        "masseria crime family", # defunct
        "masseria faction", # defunct
        "masseria mafia", # defunct
        "castellammarese clan",                          # defunct
        "castellammarese faction",                       # defunct
        "the firm",                                      # defunct
        "los pepes",                                     # defunct
        "volksfront",                                    # defunct
        "orekhovskaya gang",                             # defunct
        "orekhovskaya opg",                              # defunct
        "orekhovskaya organized crime group",            # defunct
        "st. louis crime family",                        # defunct
        "dhak-duhre group",                              # defunct
        "cártel independiente de acapulco",              # defunct
        "independent cartel of acapulco", #defunct
        "aryan nations",                                 # defunct
        "the order",                                     # defunct
        "ivankov's gang",                                # defunct
        "năm cam's organization",                        # defunct
        "năm cam's gang", #defunct
        "dung hà's gang",                                # defunct
        "los lobos",                                     # defunct
        "la corporación",                                # defunct
        "lago clan",                                     # defunct
        "acdegam",                                       # defunct
        "asociación campesina de ganaderos y agricultores del magdalena medio", #defunct
        "big seven",                                     # defunct
        "boston mafia family",                           # defunct
        "caravan of death",                              # defunct
        "chambers brothers",                             # defunct
        "clan arlistico-terracciano-orefice",            # defunct
        "clan di biasi",                                 # defunct
        "clan imparato",                                 # defunct
        "clan lago",                                     # defunct
        "clarke gang",                                   # defunct
        "death riders mc",                               # defunct
        "demon keepers motorcycle club",                 # defunct
        "devil's ushers",                                # defunct
        "digregorio faction",                            # defunct
        "dömötör-kolompár criminal organization",        # defunct
        "fiumara/coppola crew",                          # defunct
        "gallo crew",                                    # defunct
        "gatto crew",                                    # defunct
        "hải bánh's gang",                               # defunct
        "iron guard",                                    # defunct
        "kintex",                                        # defunct
        "kray firm",                                     # defunct
        "los matazetas",                                 # defunct
        "los priscos",                                   # defunct
        "los texas",                                     # defunct
        "matranga crime family",                         # defunct
        "midlands outlaws",                              # defunct
        "midland outlaws", #defunct
        "morbids motorcycle club",                       # defunct
        "outcasts", #defunct
        "north coast cartel",                            # defunct
        "orthodox jewish divorce coercion gang",         # defunct
        "ride or die gang",                              # defunct
        "russian mob",                                   # defunct
        "rédoine faïd gang",                             # defunct
        "soldier blue motorcycle club",                  # defunct
        "the order ii",                                  # defunct
        "tonton macoute",                                # defunct
        "tri-city skins",                                # defunct
        "varela organization",                           # defunct
        "vietnamese trang gang",                         # defunct
        "whitney gang",                                  # defunct
        "yoshimi-kogyo",                                 # defunct
        "young boys incorporated",                       # defunct
        "bouyakhrichan organization",                    # defunct
        "bouyakhrichan organisation", #defunct
        "aslan usoyan's organization",                   # defunct
        "usoyan's network",                              # defunct
        "bronx '95",                                     # defunct
        "shower posse",                                  # defunct
        "cárteles unidos",                               # defunct
        "grupo 27",                                      # defunct
        "westies",                                       # defunct
        "turatello crew",                                # defunct
        "lubrano-ligato clan",                           # defunct
        "abu nidal organization",                        # defunct
        "abu nidal", # defunct
        "black september",                               # defunct
        "black september movement", # defunct
        "japanese red army",                             # defunct
        "eta",                                           # defunct
        "euskadi ta askatasuna",  # defunct
        "basque separatist group eta", # defunct
        "irish national liberation army",                # defunct
        "ulster volunteer force",                        # defunct
        "ulster defence association",                    # defunct
        "red hand commando",                             # defunct
        "magaddino crime family",                        # defunct
        "magaddino clan",                                # defunct
        "the mau maus",                                  # defunct
        "yondaime manabe-gumi",                          # defunct
        "13th tribe mc",                                 # defunct
        "13th tribe", #defunct
        "gypsy raiders", #defunct
        "filthy few", #defunct
        "apollos motorcycle club",                       # defunct
        "black sheep",                                   # defunct
        "branded motorcycle club",                       # defunct
        "bug and meyer mob",                             # defunct
        "bugs and meyer mob",                             # defunct
        "darksiders motorcycle club",                    # defunct
        "destroyers",                                    # defunct
        "dirty dräggels",                                # defunct
        "east coast riders",                             # defunct
        "fáfnir",                                        # defunct
        "ghostrider's",                                  # defunct
        "hell's henchmen",                               # defunct
        "iron cross club",                               # defunct
        "iron hawgs",                                    # defunct
        "kreidler ploeg oost",                           # defunct
        "last chance",                                   # defunct
        "lost breed",                                    # defunct
        "mbm gang",                                      # defunct
        "mothers mc",                                    # defunct
        "no surrender",                                  # defunct
        "popes",                                         # defunct
        "rabies mc",                                     # defunct
        "corleonesi", #defunct
        "corleonesi clan",  #defunct
        "corleonesi mafia", #defunct
        "corleonesi mafia clan", #defunct
        "cali cartel",                      # defunct
        "medellín cartel",                  # defunct
        "medellin cartel", 
        "colombian medellín cartel", #defunct
        "medellin cartel", #defunct
        "guérini clan", #defunct
        "guerini clan", #defunct
        "guerini gang", #defunct
        "dubois gang", #defunct
        "dubois brothers", #defunct
        "dubois brothers gang", #defunct
        "gooch gang", #defunct
        "gooch close gang", #defunct
        "colombian medellín cartel", #defunct
        "morello crime family",             # defunct
        "morello family",  # defunct
        "morello gang",    # defunct
        "autodefensas unidas de colombia",  # defunct
        "westies",                          # defunct
        "westies gang",                     # defunct
        "the westies",                      # defunct
        "masseria family",                  # defunct
        "abu nidal organization",           # defunct
        "eta",                              # defunct
        "black september",                  # defunct
        "bug and meyer mob",                # defunct
        "kray firm",                        # defunct
        "kray brothers",                    # defunct 
        "kray twins",                       # defunct
        "kray twins gang",                  # defunct 
        "kray twins organization",          # defunct
        "kray twins' firm",                 # defunct
        "norte del valle cartel",           # defunct
        "north valley", #defunct
        "năm cam's organization",           # defunct
        "red army faction",                 # defunct
        "los extraditables",                # defunct
        "irish mob (oklahoma)",             # defunct
        "joseph \"legs\" laman gang",       # defunct
        "kyokushinrengo-kai",               # defunct
        "la nueva administración",          # defunct
        "muslim brotherhood movement",      # defunct
        "nedim baybaşin gang",              # defunct
        "outlaw hammerskins",               # defunct
        "107th street gang",               # defunct
        "11th street chavos",              # defunct
        "14th street clovers",             # defunct
        "19th hole crew",                  # defunct
        "69th street gang",                # defunct
        "adamo gang",                      # defunct
        "ah kong",                         # defunct
        "alfred mineo's gang",             # defunct
        "alianza anticomunista argentina", # defunct
        "al-rukn",                         # defunct
        "aryan brothers",                  # defunct
        "aryan society",                   # defunct
        "ashkenazum",                      # defunct
        "aztecas",                         # defunct
        "ba thế's gang",                   # defunct
        "tín mã nàm's gang",               # defunct
        "cut downs",                       # defunct
        "first ward gang",                 # defunct
        "guérini clan", #defunct
        "jheri curls", #defunct
        "angiulo brothers", #defunct
        "anqing daoyou", #defunct
        "banda dei marsigliesi", #defunct
        "beach gang", #defunct
        "acdegam", #defunct
        "bonnot gang", #defunct
        "bình xuyên", #defunct
        "black mafia", #defunct
        "battalion 3-16", #defunct
        "bauman organized crime group", #defunct
        "bengal tigers", #defunct
        "birger gang", #defunct
        "10th & oregon crew", #defunct
        "abu sayyaf", #defunct
        "assyrian kings", #defunct
        "bouyakhrichan organization", #defunct
        "cártel independiente de acapulco", #defunct
        "chosen few motorcycle club", #defunct
        "colorado crime family", #defunct
        "corleonesi", #defunct
        "cuntrera-caruana mafia clan", #defunct
        "d'alessandro clan", #defunct
        "dubois gang", #defunct
        "gooch gang", #defunct
        "greco clan of ciaculli", #defunct
        "grey wolves", #defunct
        "grim reapers", #defunct
        "guérini clan", #defunct
        "insane deuces", #defunct
        "jokers", #defunct
        "junior black mafia", #defunct
        "k&a gang", #defunct
        "lobos", #defunct
        "longsight crew", #defunct
        "mariano clan", #defunct
        "medellín cartel", #defunct
        "nuova famiglia", #defunct
        "peaky blinders", #defunct
        "popeye moto club", #defunct
        "popeyes motorcycle club", #defunct
        "popeye motorcycle club", #defunct
        "santa maria di gesù mafia family", #defunct
        "santa maria di gesù family", #defunct
        "seven immortals", #defunct
        "tupamaro", #defunct
        "winter hill gang", #defunct
        "đại cathay's gang", #defunct
        "albanian boys", #defunct
        "atf", #defunct
        "bing kong tong", #defunct
        "birmingham boys", #defunct
        "bouyakhrichan organization", #defunct
        "cártel independiente de acapulco", #defunct
        "cheetham hill gang", #defunct
        "clan magliulo", #defunct
        "cortesi brothers", #defunct
        "cutoliani", #defunct
        "elephant and castle mob", #defunct
        "farc", #defunct
        "farc-ep", #defunct
        "revolutionary armed forces of colombia", #defunct
        "revolutionary armed forces of colombia (farc)", #defunct
        "fuerzas armadas revolucionarias de colombia", #defunct
        "revolutionary armed forces of colombia – people's army", #defunct
        "guérini clan", #defunct
        "hoxton gang", #defunct
        "kompania bello", #defunct
        "lrgp", #defunct
        "malyshevskaya ogg", #defunct
        "medellín cartel", #defunct
        "neapolitan faction (newark)", #defunct
        "neighbourhood compton crips", #defunct
        "nuova camorra organizzata", #defunct
        "original red devils motorcycle club", #defunct
        "palermo's gang", #defunct
        "ping on", #defunct
        "popeyes", #defunct
        "popeyes motorcycle club", #defunct
        "sabinis", #defunct
        "santa maria di gesù mafia family", #defunct
        "south brooklyn boys", #defunct
        "spook hunters", #defunct
        "stanfa faction", #defunct
        "suey sing tong", #defunct
        "tupamaro", #defunct
        "vitale gang", #defunct
        "warlords motorcycle club", #defunct
        "white tigers", #defunct
        "wo hop to", #defunct
        "zaza clan", #defunct
        "zemun clan", #defunct
        "đại cathay's gang" #defunct
        "đại Cathay's organization", #defunct
        "đại cathay's organization"
    }

    before = len(nodes)
    nodes = [n for n in nodes if n["standard_name"].strip().lower() not in TO_BE_EXCLUDED]
    removed = before - len(nodes)
    if removed:
        log.info(f"Removed {removed} non-criminal entities")

    # 1b. Fix string "null" in nullable fields (LLM outputs "null" as string)
    null_strings = {"null", "none", "unknown", "n/a", ""}
    tp_fixed = 0
    for node in nodes:
        tp = node.get("time_period")
        if isinstance(tp, str) and tp.strip().lower() in null_strings:
            node["time_period"] = None
            tp_fixed += 1
    for edge in edges:
        tp = edge.get("time_period")
        if isinstance(tp, str) and tp.strip().lower() in null_strings:
            edge["time_period"] = None
            tp_fixed += 1
        d = edge.get("detail")
        if isinstance(d, str) and d.strip().lower() in null_strings:
            edge["detail"] = None
    if tp_fixed:
        log.info(f"String null sanitization: {tp_fixed} time_period values fixed")

    # 2. Sanitize relationships and consolidate detail types
    VALID_RELS = {"alliance", "rivalry", "other"}
    rc, dc, reclass = 0, 0, 0
    for edge in edges:
        rel = (edge.get("relationship") or "").strip().lower()

        # Fix invalid relationship values (LLM hallucinations like "sinaloa cartel")
        if rel not in VALID_RELS:
            edge["relationship"] = "other"
            rc += 1
            rel = "other"

        detail = (edge.get("detail") or "").strip().lower()

        # Reclassify: if relationship is "other" but detail is really alliance/rivalry
        if rel == "other" and detail:
            if detail in DETAIL_TO_ALLIANCE:
                edge["relationship"] = "alliance"
                edge["detail"] = None
                reclass += 1
                continue
            elif detail in DETAIL_TO_RIVALRY:
                edge["relationship"] = "rivalry"
                edge["detail"] = None
                reclass += 1
                continue

        # Consolidate remaining detail types
        if edge.get("detail"):
            old = edge["detail"]
            new = consolidate_detail(old)
            if old != new:
                dc += 1
            edge["detail"] = new

    if rc:
        log.info(f"Relationship sanitization: {rc} invalid values fixed")
    log.info(f"Detail reclassification: {reclass} edges moved to alliance/rivalry")
    log.info(f"Detail consolidation: {dc} remapped")

    # 3. Dedup
    merge_map, groups = build_dedup_map(nodes)
    if groups:
        log.info(f"Dedup: {len(groups)} duplicate groups:")
        for group in groups:
            canonical = min(group, key=len)
            log.info(f"  '{canonical}' ← {group - {canonical}}")

    final_nodes = {}
    for node in nodes:
        name = node["standard_name"]
        canonical = merge_map.get(name, name)
        node["standard_name"] = canonical
        key = normalize(canonical)
        if key not in final_nodes:
            final_nodes[key] = node
        else:
            existing = final_nodes[key]
            existing["aliases"] = sorted(set(existing.get("aliases", []))
                                         | set(node.get("aliases", []))
                                         | ({name} if name != canonical else set()))
            existing["wikipedia_urls"] = sorted(set(existing.get("wikipedia_urls", []))
                                                 | set(node.get("wikipedia_urls", [])))
            existing["source_articles"] = sorted(set(existing.get("source_articles", []))
                                                  | set(node.get("source_articles", [])))
            if len(node.get("context", "")) > len(existing.get("context", "")):
                existing["context"] = node["context"]
            if len(node.get("time_period") or "") > len(existing.get("time_period") or ""):
                existing["time_period"] = node["time_period"]
    nodes = list(final_nodes.values())

    # 1a. Force specific node type overrides
    NODE_TYPE_OVERRIDES = {
        # organization → criminal_organization
        "alleanza di secondigliano": "criminal_organization",
        "atf": "criminal_organization",
        "axis of resistance": "criminal_organization",
        "locos 13 alliance": "criminal_organization",
        "nuova famiglia": "criminal_organization",
        "secondigliano alliance": "criminal_organization",
        "the golden triangle alliance": "criminal_organization",
        # criminal_organization → mafia
        "albanian mafia": "mafia",
        "amber mafia": "mafia",
        "bouncer mafia": "mafia",
        "bulgarian mafia": "mafia",
        "chechen mafia gangs": "mafia",
        "frankfurt mafia": "mafia",
        "galician mafia": "mafia",
        "iranian mafia": "mafia",
        "israeli mafia in new york": "mafia",
        "nigerian mafia": "mafia",
        "north macedonian mafia": "mafia",
        "odesa mafia": "mafia",
        "serbian mafia": "mafia",
        "tobacco mafia": "mafia",
        "yugoslav mafia": "mafia",
        "anthony perish criminal gang network": "gang",
        "bulla felix's gang": "gang",
        "corsican gangsters": "gang",
        "counter-revolutionary gangs": "gang",
        "dolgoprudnenskaya gang": "gang",
        "immigrant gangs": "gang",
        "irish gangsters based in spain": "gang",
        "italian immigrant criminal gangs": "gang",
        "italian-american gangs": "gang",
        "italian-american gangsters": "gang",
        "ivankov's gang": "gang",
        "koose munusamy veerappan gang": "gang",
        "liverpool gangs": "gang",
        "maghrebian gangs": "gang",
        "mazutka gang": "gang",
        "melbourne gangland killings participants": "gang",
        "middle eastern/lebanese organized crime gangs": "gang",
        "moss side gangs": "gang",
        "new york-based drug gangs": "gang",
        "nigerian pirate gangs": "gang",
        "orthodox jewish divorce coercion gang": "gang",
        "other costa mesa gangs": "gang",
        "philip paul's gang": "gang",
        "pirate gangs": "gang",
        "podolskaya gang": "gang",
        "reservoir gang": "gang",
        "rosenzweig's gang": "gang",
        "sam catalanotte gang": "gang",
        "slonovskaya gang": "gang",
        "smaller drug gangs": "gang",
        "socialist revolutionary gangs": "gang",
        "somali pirate gangs": "gang",
        "tài chém's gang": "gang",
        "usvyatsov-putyrsky gang": "gang",
        "waray-waray gangs": "gang",
        "waxey gordon's gang": "gang",
        "yardie gangs": "gang",
        "varador cortet": "gang",
        "inzerillo-gambino mafia clan": "mafia",
        "miri clan": "clan",
        "remmo clan": "clan",
        "fakhro clan": "clan",  
        "giuliano-mazzarella cartel": "cartel",
        "beltrán-leyva cartel": "cartel",
        "bolivarian cartel": "cartel",
        "cartel of the suns": "cartel",
        "fuerzas armadas de liberación nacional": "terrorist_organization",  # FALN — currently "criminal_organization" which isn't a valid type
        "big circle gang": "gang",
        "birger gang": "gang",
        "bonnot gang": "gang",
        "charlestown gang": "gang",
        "cheetham hill gang": "gang",
        "chotta rajan gang": "gang",
        "city hall gang": "gang",
        "cochise cowboy gang": "gang",
        "down river gang": "gang",
        "dubois gang": "gang",
        "dung hà's gang": "gang",
        "eastside gang": "gang",
        "felony lane gang": "gang",
        "fuqing gang": "gang",
        "gang groups supporting captain triệu": "gang",
        "gangster nation": "gang",
        "gangsters": "gang",
        "golden boys gang": "gang",
        "hunan gang": "gang",
        "hùng 'the warbler's gang": "gang",
        "hùng cốm's gang": "gang",
        "hải bánh's gang": "gang",
        "irish west end gang": "gang",
        "karachi-based gangs": "gang",
        "kazan gang": "gang",
        "malyshevskaya gang": "gang",
        "metz gang": "gang",
        "numbers gang": "gang",
        "o'neill gang": "gang",
        "other gangs from hanoi, namdinh, and haiphong": "gang",
        "quality street gang": "gang",
        "shelton brothers gang": "gang",
        "son bach tang's gang": "gang",
        "tambov gang": "gang",
        "the johnson gang": "gang",
        "thuggee gangs": "gang",
        "turatello gang": "gang",
        "vallanzasca gang": "gang",
        "velikolukskaya gang": "gang",
        "west end gang": "gang",
        "clan del golfo": "clan",
        "alkhalil family": "crime_family",
        "black mafia family": "crime_family",
        "manson family": "crime_family",
        "the family": "crime_family",
        "white family": "crime_family",
        "yangeuni family": "crime_family",
        "zoe mafia family": "crime_family",
        "brew crew": "crew",
        "c-crew": "crew",
        "crew 38": "crew",
        "shadowcrew": "crew",
        "the carlton crew": "crew",
        "young firm crew": "crew",
        "american mafia": "mafia",
        "balkan mafia": "mafia",
        "black mafia": "mafia",
        "cornbread mafia": "mafia",
        "cowboy mafia": "mafia",
        "indian mafia": "mafia",
        "irish mafia": "mafia",
        "jewish mafia": "mafia",
        "jewish-american mafia": "mafia",
        "mafia capitale": "mafia",
        "mexican mafia": "mafia",
        "montenegrin mafia": "mafia",
        "national mafia syndicate": "mafia",
        "new mafia": "mafia",
        "portuguese mafia": "mafia",
        "red mafia": "mafia",
        "sicilian mafia commission": "mafia",
        "sicilian mafia families": "mafia",
        "sin city mafia": "mafia",
        "slovak mafia": "mafia",
        "the commission (american mafia)": "mafia",
        "timber mafia": "mafia",
        "timber mafia from assam": "mafia",
        "timber mafia in brazil": "mafia",
        "timber mafia in cambodia": "mafia",
        "timber mafia in congo": "mafia",
        "timber mafia in india": "mafia",
        "timber mafia in indonesia": "mafia",
        "timber mafia in romania": "mafia",
        "210 international": "gang",
        "400 mawozo": "gang",
        "ah kong": "cartel",
        "ait soussan organization": "cartel",
        "alleanza di secondigliano": "clan",
        "alperen hearths": "militia",
        "alperon crime organization": "crime_family",
        "amigos dos amigos": "gang",
        "amirante-brunetti-sibillo": "faction",
        "anthony grosso organization": "crime_family",
        "arakan army": "militia",
        "armenian organized crime group": "clan",
        "armenian power": "gang",
        "aryan brotherhood": "gang",
        "aslan usoyan's organization": "mafia",
        "aston villa hardcore": "gang",
        "aston villa hooligan firms": "gang",
        "atf": "gang",
        "australian-albanian organized crime": "clan",
        "awb": "militia",
        "az syndicate": "crime_family",
        "bacon brothers": "gang",
        "bacrim": "cartel",
        "banda dei marsigliesi": "gang",
        "banda della comasina": "gang",
        "banda della magliana": "mafia",
        "bears brigade": "militia",
        "berman brothers": "crime_family",
        "bing kong tong": "triad",
        "black eagles": "militia",
        "black hand": "mafia",
        "blood & honour": "terrorist_organization",
        "blood & honour australia": "terrorist_organization",
        "bloque centauros": "militia",
        "bloque meta": "militia",
        "blue-and-black movement": "gang",
        "bonde do maluco": "gang",
        "bonde dos 40": "gang",
        "bouyakhrichan organization": "cartel",
        "buccaneers confraternity": "gang",
        "bình xuyên": "militia",
        "bảy viễn's bình xuyên": "faction",
        "cali drug-trafficking groups": "cartel",
        "campbell brothers": "gang",
        "cardiff city hooligans": "gang",
        "celtic club": "crew",
        "chao pho": "gang",
        "chelsea headhunters": "gang",
        "christian front": "militia",
        "cida": "cartel",
        "clerkenwell crime syndicate": "gang",
        "cleveland syndicate": "crime_family",
        "coalition of patriots for change": "militia",
        "comando classe a": "gang",
        "comando vermelho": "cartel",
        "combat 18 serbia": "militia",
        "compton executioners": "gang",
        "counter-guerrilla": "militia",
        "county road cutters": "gang",
        "cutoliani": "clan",
        "cárteles unidos": "cartel",
        "dalen network": "gang",
        "dark circle": "motorcycle_club",
        "dev-sol": "militia",
        "dhak group": "gang",
        "dhak-duhre group": "cartel",
        "dhure group": "gang",
        "district 61": "gang",
        "dogpack 416": "gang",
        "doktor's bratva": "crime_family",
        "duhre brothers": "gang",
        "duhre crime group": "gang",
        "eiye confraternity": "gang",
        "elohim city": "militia",
        "espanola formation": "militia",
        "faln": "terrorist_organization",
        "familia do norte": "gang",
        "família do norte": "cartel",
        "far-ept": "militia",
        "farp": "terrorist_organization",
        "farruku crime group": "gang",
        "fenians": "terrorist_organization",
        "fernando pineda-jimenez organization": "cartel",
        "finnish blood & honour": "terrorist_organization",
        "fourth armoured division": "militia",
        "foxtrot network": "cartel",
        "fuerza anti-unión": "faction",
        "fujianese organized crime": "triad",
        "g-pèp": "militia",
        "ghee hin kongsi": "triad",
        "ghee hin society": "triad",
        "gran familia mexicana": "gang",
        "group america": "cartel",
        "group of popular combatants": "militia",
        "grupo 27": "gang",
        "grupo bravo": "faction",
        "grupo escorpión": "cartel",
        "grupo sombra": "cartel",
        "guardia michoacana": "cartel",
        "guardiões do estado": "gang",
        "hai san secret society": "triad",
        "hai san society": "triad",
        "hammerskin nation": "gang",
        "hammerskins": "terrorist_organization",
        "hassan daqou network": "cartel",
        "herrera organization": "cartel",
        "hip sing association": "triad",
        "hip sing tong": "triad",
        "honghuzi": "gang",
        "honoured society": "mafia",
        "hung mong": "triad",
        "ichiwa-kai": "faction",
        "ignacio antinori's organization": "mafia",
        "insane familia": "faction",
        "inter city firm": "gang",
        "irish mob": "cartel",
        "iron blood patriots": "gang",
        "italian crime syndicates": "mafia",
        "italian mob": "mafia",
        "italian organized crime groups": "mafia",
        "jamaican posse": "gang",
        "japanese yakuza": "mafia",
        "jewish mob": "crime_family",
        "jewish-american mob": "crime_family",
        "jewish-american organized crime": "crime_family",
        "kachin independence army": "militia",
        "karen national liberation army": "militia",
        "karenni national people's liberation front": "militia",
        "karenni national progressive party": "militia",
        "kayan new land party": "militia",
        "keystone united": "terrorist_organization",
        "kobe yamaguchi-gumi": "faction",
        "komarovskaya organized criminal group": "clan",
        "kompania bello": "mafia",
        "koo majok's mob": "mafia",
        "kop of boulogne": "gang",
        "kosovar-albanian crime groups": "clan",
        "kray firm": "gang",
        "kuratong baleleng": "militia",
        "kutaisi criminal group": "clan",
        "kyōsei-kai": "gang",
        "kërtalla crime group": "clan",
        "la conexión": "cartel",
        "la corporación": "cartel",
        "la empresa": "cartel",
        "la mayiza": "cartel",
        "la nueva empresa": "cartel",
        "la oficina de envigado": "cartel",
        "la onu": "gang",
        "la raza nation": "gang",
        "la rompe onu": "cartel",
        "la unión tepito": "cartel",
        "le milieu": "mafia",
        "leroy barnes network": "gang",
        "libertadores del vichada": "faction",
        "licavoli squad": "crime_family",
        "liverpool fc hooligans": "gang",
        "lo san": "triad",
        "los antrax": "cartel",
        "los blancos de la troya": "cartel",
        "los blancos de troya": "cartel",
        "los cachiros": "cartel",
        "los cazadores": "cartel",
        "los chone killers": "gang",
        "los choneros": "cartel",
        "los contrabandistas": "cartel",
        "los correa": "cartel",
        "los kilos": "gang",
        "los lagartos": "cartel",
        "los lobos": "cartel",
        "los malcriados 3ad": "gang",
        "los pepes": "militia",
        "los páez": "crew",
        "los queseros": "gang",
        "los rastrojos": "cartel",
        "los tanzanios": "gang",
        "los tiguerones": "cartel",
        "luhansk people's republic": "militia",
        "luppino-violi group": "crime_family",
        "luton town migs": "gang",
        "maceo organization": "crime_family",
        "maceo syndicate": "crime_family",
        "machos": "militia",
        "malhi-buttar coalition": "gang",
        "maniac famila": "faction",
        "maphite confraternity": "gang",
        "marielitos": "cartel",
        "martha organization": "clan",
        "mau maus": "gang",
        "mayfield road mob": "crime_family",
        "medellín drug-trafficking groups": "cartel",
        "millwall bushwackers": "gang",
        "minneapolis combination": "mafia",
        "minneapolis syndicate": "gang",
        "mocro maffia": "mafia",
        "mon national liberation army": "militia",
        "mong tai army": "militia",
        "moroccan drug barons": "cartel",
        "mountain church of jesus christ the savior": "militia",
        "mpcp": "militia",
        "murder, inc.": "crew",
        "national alliance": "terrorist_organization",
        "national crime syndicate": "crime_family",
        "national democratic alliance army": "militia",
        "national knights of the ku klux klan": "terrorist_organization",
        "neapolitan crime bosses": "mafia",
        "nicaraguan drug dealers": "cartel",
        "noble elect thugs": "gang",
        "nordic resistance movement": "terrorist_organization",
        "northern structure": "gang",
        "nuova camorra organizzata": "mafia",
        "nuova famiglia": "mafia",
        "nuova famiglia salentina": "clan",
        "nuova grande camorra pugliese": "clan",
        "năm cam's organization": "gang",
        "okaida": "gang",
        "on leong chinese merchants association": "triad",
        "on leong tong": "triad",
        "organisation of taghi": "cartel",
        "organizacion de narcotraficantes unidos": "cartel",
        "outlaw hammerskins": "faction",
        "paisas": "militia",
        "paranza dei bambini": "clan",
        "pce-sr": "militia",
        "pcp-cbmr": "militia",
        "peasant self-defense forces of casanare": "militia",
        "penose": "gang",
        "philadelphia greek mob": "faction",
        "piezzo group": "clan",
        "pindaris": "militia",
        "pink panthers": "clan",
        "polish mob": "mafia",
        "post-soviet organized crime groups": "mafia",
        "preman": "gang",
        "primeiro comando de eunápolis": "gang",
        "primeiro comando do maranhão": "gang",
        "pueblos unidos": "militia",
        "quds force": "militia",
        "radev bratva": "clan",
        "red army": "gang",
        "red legions stuttgart": "gang",
        "red wa": "triad",
        "remo lecce libera": "clan",
        "revolutionary forces of the g9 family and allies": "militia",
        "rise above movement": "gang",
        "rudaj organization": "gang",
        "russian mob": "mafia",
        "russian separatist forces in donbas": "militia",
        "rustavi criminal group": "clan",
        "sabinis": "gang",
        "sacra corona unita": "mafia",
        "sam clay organization": "gang",
        "sam gor": "cartel",
        "secondigliano alliance": "clan",
        "semion mogilevich crime network": "mafia",
        "semion mogilevich organization": "mafia",
        "serbian organized crime group": "gang",
        "serbian paramilitaries in kosovo": "militia",
        "shan state army": "militia",
        "shan state national army": "militia",
        "shapiro brothers": "crew",
        "sic": "clan",
        "siderno group": "mafia",
        "sio sam ong": "triad",
        "snakeheads": "triad",
        "società foggiana": "mafia",
        "solntsevskaya bratva": "mafia",
        "south african crime lords": "cartel",
        "southern cross hammerskins": "gang",
        "state line mob": "gang",
        "stidda": "mafia",
        "sudanese revolutionary front": "militia",
        "suey sing tong": "gang",
        "supreme eiye confraternity": "gang",
        "syndicate": "gang",
        "ta'ang national liberation army": "militia",
        "taghi organisation": "cartel",
        "taghi organization": "cartel",
        "tamazuj": "militia",
        "tariel oniani organized crime group": "clan",
        "tariel oniani's organization": "clan",
        "tbilisi criminal group": "clan",
        "terceiro comando": "gang",
        "terceiro comando puro": "gang",
        "the commission": "crime_family",
        "the council": "gang",
        "the enterprise": "militia",
        "the firm": "gang",
        "the syndicate": "motorcycle_club",
        "thief in law": "mafia",
        "thuggee": "gang",
        "tiandihui": "triad",
        "tong": "triad",
        "toronto 'ndranghetisti": "mafia",
        "tung on association": "gang",
        "tupamaro": "militia",
        "turkish groups": "cartel",
        "union corse": "mafia",
        "united klans of america": "militia",
        "united latino organization": "gang",
        "united liberation front of assam": "militia",
        "united tribuns": "gang",
        "usoyan's network": "mafia",
        "vancouver mob": "cartel",
        "vanella grassi": "clan",
        "varela organization": "cartel",
        "various latin american drug trafficking organizations": "cartel",
        "velentzas organization": "clan",
        "veren laki": "militia",
        "vietnamese drug trafficking organisations": "cartel",
        "vis": "clan",
        "volksfront": "terrorist_organization",
        "volunteers": "terrorist_organization",
        "vory v zakone": "mafia",
        "vyacheslav ivankov organization": "mafia",
        "wa national army": "militia",
        "wagner group": "militia",
        "white patriot party": "militia",
        "wo on lok": "triad",
        "wolf pack": "gang",
        "wolfpack alliance": "cartel",
        "yacs": "clan",
        "yakuza": "mafia",
        "yamaguchi-gumi": "mafia",
        "yardies": "gang",
        "yugoslavian crime syndicate": "clan",
        "zoe nation": "gang",
        "zulu warriors": "gang",
        "gambino crime family": "mafia",
        "brooklyn faction (lucchese crime family)": "mafia",
        "lucchese crime family": "mafia",
        "papalia crime family": "mafia",
        "116th street crew": "mafia",
        "abate clan": "mafia",
        "acquasanta mafia clan": "mafia",
        "amato-pagano clan": "mafia",
        "american mafia crime families": "mafia",
        "aquilino 'ndrina": "mafia",
        "balistrieri family": "mafia",
        "barbaro 'ndrina": "mafia",
        "bonanno crime family": "mafia",
        "bontade mafia family": "mafia",
        "bruno-scarfo mafia crime family": "mafia",
        "bufalino crime family": "mafia",
        "buffalo crime family": "mafia",
        "buffalo mafia": "mafia",
        "caltagirone mafia family": "mafia",
        "castellammare del golfo mafia family": "mafia",
        "catania mafia family": "mafia",
        "chicago mafia": "mafia",
        "chicago outfit": "mafia",
        "cleveland crime family": "mafia",
        "colombo crime family": "mafia",
        "commisso 'ndrina": "mafia",
        "corleonesi": "mafia",
        "decavalcante crime family": "mafia",
        "detroit partnership": "mafia",
        "five families": "mafia",
        "gambino crime family": "mafia",
        "genna crime family": "mafia",
        "genovese crime family": "mafia",
        "inzerillo crime family": "mafia",
        "kansas city crime family": "mafia",
        "larocca crime family": "mafia",
        "los angeles crime family": "mafia",
        "lucchese crime family": "mafia",
        "madonia mafia family": "mafia",
        "magaddino crime family": "mafia",
        "mammoliti 'ndrina": "mafia",
        "mancuso 'ndrina": "mafia",
        "mazzarella clan": "mafia",
        "milwaukee crime family": "mafia",
        "motisi mafia clan": "mafia",
        "new england crime family": "mafia",
        "new orleans crime family": "mafia",
        "passo di rigano mafia family": "mafia",
        "patriarca crime family": "mafia",
        "pelle 'ndrina": "mafia",
        "pesce-bellocco 'ndrina": "mafia",
        "philadelphia crime family": "mafia",
        "pittsburgh crime family": "mafia",
        "resuttana mafia family": "mafia",
        "rizzuto crime family": "mafia",
        "san francisco crime family": "mafia",
        "san jose crime family": "mafia",
        "santa maria di gesù mafia family": "mafia",
        "santapaola mafia family": "mafia",
        "st. louis crime family": "mafia",
        "tampa crime family": "mafia",
        "tegano 'ndrina": "mafia",
        "the commission": "mafia",
        "trafficante crime family": "mafia",
        "villabate mafia family": "mafia",
        "vollaro clan": "mafia",
        "zaza clan": "mafia",
        "zerilli crime family": "mafia",
        "angiuolo brothers": "mafia",
        "antonio bardellino": "mafia",
        "badami family": "mafia",
        "badami newark family": "mafia",
        "birra clan": "mafia",
        "bordonaro family": "mafia",
        "boston mafia family": "mafia",
        "bruno crime family": "mafia",
        "calabrese family": "mafia",
        "calderone mafia family": "mafia",
        "capriati clan": "mafia",
        "chicago families": "mafia",
        "clan amato-pagano": "mafia",
        "clan anastasio": "mafia",
        "clan arlistico-terracciano-orefice": "mafia",
        "clan de luca bossa": "mafia",
        "clan misso": "mafia",
        "clan panico": "mafia",
        "clan ricci": "mafia",
        "clan veneruso": "mafia",
        "coluccio crime family": "mafia",
        "corso dei mille family": "mafia",
        "cosoleto crime family": "mafia",
        "cotroni crime family": "mafia",
        "d'aquila family": "mafia",
        "dallas crime family": "mafia",
        "demaria crime family": "mafia",
        "denver crime family": "mafia",
        "detroit crime family": "mafia",
        "di biasi clan": "mafia",
        "di maggio family": "mafia",
        "esposito-genidoni clan": "mafia",
        "figliomeni crime family": "mafia",
        "genna brothers": "mafia",
        "gravina crime family": "mafia",
        "la barbera brothers": "mafia",
        "luciano-genovese family": "mafia",
        "luppino crime family": "mafia",
        "luppino family": "mafia",
        "luppino-violi group": "mafia",
        "mafia families of hamilton": "mafia",
        "mafia family of mariano agate": "mafia",
        "maisto clan": "mafia",
        "marando clan": "mafia",
        "matranga family": "mafia",
        "matrone clan": "mafia",
        "mazzei clan": "mafia",
        "mesagnesi clan": "mafia",
        "michele greco": "mafia",
        "michele zaza": "mafia",
        "morello crime family": "mafia",
        "motisi mafia family": "mafia",
        "nardo clan": "mafia",
        "new england family": "mafia",
        "new york families": "mafia",
        "new york mafia": "mafia",
        "newark family": "mafia",
        "newark family (mafia)": "mafia",
        "orlando clan": "mafia",
        "padovano clan": "mafia",
        "palermo centro mafia family": "mafia",
        "palermo-based mafia families of stefano bontade and salvatore inzerillo": "mafia",
        "parisi clan": "mafia",
        "parisi-palermiti clan": "mafia",
        "pecoraro-renna clan": "mafia",
        "philadelphia family": "mafia",
        "piccirillo clan": "mafia",
        "porrello crime family": "mafia",
        "prestieri clan": "mafia",
        "provenzano family": "mafia",
        "providence mafia family": "mafia",
        "puccio family": "mafia",
        "ramacca mafia family": "mafia",
        "renzvillo crime family": "mafia",
        "rochester crime family": "mafia",
        "rockford crime family": "mafia",
        "rogoli-buccarella-donatiello clan": "mafia",
        "romeo clan": "mafia",
        "ruga 'ndrina": "mafia",
        "russo del rione traiano": "mafia",
        "russo di fuorigrotta": "mafia",
        "san giuseppe jato mafia family": "mafia",
        "santa maria di gesù family": "mafia",
        "santapaola family": "mafia",
        "santapaola mafia clan": "mafia",
        "santapaola-ercolano network": "mafia",
        "scarfo crime family": "mafia",
        "schirò family": "mafia",
        "seattle crime family": "mafia",
        "serino clan": "mafia",
        "spatola family": "mafia",
        "springfield ma genovese crew": "mafia",
        "stabile clan": "mafia",
        "strisciuglio clan": "mafia",
        "suraci family": "mafia",
        "tagliavia family": "mafia",
        "tavernese crime family": "mafia",
        "tom gagliano's family": "mafia",
        "tornese clan": "mafia",
        "torretta mafia family": "mafia",
        "trimboli clan": "mafia",
        "utica crime family": "mafia",
        "vallelunga pratameno mafia family": "mafia",
        "vastarella clan": "mafia",
        "veneruso clan": "mafia",
        "violi family": "mafia",
        "zappia clan": "mafia",
        "angiulo brothers": "mafia",
        "castellano family": "mafia",
        "famiglia bisogni": "mafia",
        "famiglia frappaolo": "mafia",
        "famiglia giffoni": "mafia",
        "famiglia maiale": "mafia",
        "famiglia mogavero": "mafia",
        "famiglia trimarco": "mafia",
        "langana crime family": "mafia",
        "mannino family": "mafia",
        "matranga crime family": "mafia",
        "zoe mafia family": "mafia",
        "clan sacco-bocchetti": "mafia",
        "colonna crime family": "mafia",
        "colorado crime family": "mafia",
        "cozzilino clan": "mafia",
        "sciorio family": "mafia",
        'texas mafia': "gang",
        "a.z. syndicate": "crew",
        "abergil crime family": "clan",
        "abu latif crime family": "clan",
        "abutbul crime family": "clan",
        "adams family": "crew",
        "adiwal brothers": "crew",
        "ahmad family": "clan",
        "alameddine crime family": "clan",
        "alkhalil family": "clan",
        "alperon crime family": "clan",
        "alperon crime organization": "clan",
        "ancelotti crime family": "crew",
        "anthony grosso organization": "crew",
        "arif family": "crew",
        "az syndicate": "crew",
        "baltimore crew": "crew",
        "baybaşin family": "clan",
        "berman brothers": "crew",
        "black mafia family": "gang",
        "blood in, blood out": "gang",
        "brindle family": "crew",
        "brindles": "crew",
        "buttar family": "gang",
        "changachi family": "clan",
        "cleveland syndicate": "crew",
        "cohen crime family": "crew",
        "corbi family": "crew",
        "d'urso family": "crew",
        "daniel crime family": "crew",
        "doktor's bratva": "crew",
        "dumrani crime family": "clan",
        "dömötör-kolompár criminal organization": "clan",
        "elmir family": "clan",
        "faial family": "crew",
        "fitzgibbon family": "crew",
        "flynn crime family": "crew",
        "georgakopoulos–psihogios-voidonikolas–leoutsakos laconian canadian families": "clan",
        "grendon crime family": "crew",
        "hamze crime family": "clan",
        "hamzy/hamze crime family": "clan",
        "haouchar crime family": "clan",
        "hornec gang": "clan",
        "jarushi crime family": "clan",
        "jewish mob": "faction",
        "jewish-american mob": "faction",
        "jewish-american organized crime": "faction",
        "johal family": "gang",
        "joseph family": "crew",
        "kang crime family": "gang",
        "kang crime family (bibo gang)": "gang",
        "leisure crime family": "crew",
        "licavoli squad": "crew",
        "liverpool crime family": "crew",
        "lyons crime family": "crew",
        "maceo organization": "faction",
        "maceo syndicate": "faction",
        "madison crime family": "crew",
        "primeiro comando da capital": "faction",
        "manson family": "faction",
        "martinez familia sangeros": "gang",
        "mayfield road mob": "crew",
        "mckenna crime family": "crew",
        "michaels crime family": "crew",
        "mickey cohen crime family": "crew",
        "moran family": "crew",
        "musitano crime family": "clan",
        "national crime syndicate": "faction",
        "noonans": "crew",
        "parasyris family": "clan",
        "pettingill families": "clan",
        "pettingill family": "clan",
        "qarajah crime family": "clan",
        "ruso crime family": "crew",
        "sabini family": "gang",
        "sanghera crime family": "gang",
        "sayers": "crew",
        "shirazi crime family": "clan",
        "suishin-kai": "faction",
        "the family": "faction",
        "velentzas crime family": "crew",
        "walkers": "crew",
        "white family": "crew",
        "whitney gang": "gang",
        "yangeuni family": "crew",
        "zemmour crime family": "clan",
        "11th street chavos": "gang",
        "17th street locas": "gang",
        "17th street tiny locos": "gang",
        "2000 boys": "gang",
        "20th streeters": "gang",
        "21st. deadend winos": "gang",
        "3000 boys": "gang",
        "akhmat units": "militia",
        "al-ittihad al-islamiya": "terrorist_organization",
        "apex": "gang",
        "arakan liberation army": "militia",
        "arakan rohingya salvation army": "militia",
        "arthur thompson syndicate": "crew",
        "asano-gumi": "gang",
        "atlantic city skinheads": "gang",
        "avilés criminal organization": "cartel",
        "aztecas": "gang",
        "ba dương's bình xuyên": "faction",
        "barahama alih group": "gang",
        "barska ocg": "clan",
        "big seven": "faction",
        "billy hill organization": "crew",
        "black and tans": "militia",
        "black axe": "gang",
        "black dogs": "gang",
        "black hand extortion rings": "crew",
        "black hand organizations": "gang",
        "black hundreds": "militia",
        "bloods & crips": "gang",
        "bloque elmer cardenas": "militia",
        "bluenoroff": "crew",
        "botanical youth club": "militia",
        "bran.co": "faction",
        "bredängs network": "gang",
        "brigada blanca": "militia",
        "bro-network": "gang",
        "brothers' circle": "clan",
        "budvanska ocg": "clan",
        "bulgarian cocaine trafficking group": "cartel",
        "bạch hải đường's robber band": "crew",
        "calabrian organization": "crew",
        "callejeros": "gang",
        "caveira": "faction",
        "celestial way": "triad",
        "chin liberation army": "militia",
        "chin national army": "militia",
        "chinland defence force": "militia",
        "combat 18 australia": "terrorist_organization",
        "combat 18 deutschland": "terrorist_organization",
        "combat 18 finland": "terrorist_organization",
        "combat 18 flanders": "terrorist_organization",
        "combat 18 greece": "terrorist_organization",
        "confederate hammerskins": "gang",
        "corsican mob of marseille": "faction",
        "crickets": "gang",
        "crime syndicate of rio grande do norte": "faction",
        "cut downs": "gang",
        "d-company": "mafia",
        "dagestani bratva": "mafia",
        "delmas 95": "gang",
        "devil's click": "gang",
        "direct action against drugs": "terrorist_organization",
        "disciple alliance": "faction",
        "dorćol group": "crew",
        "erpac": "militia",
        "esquadrão da morte": "militia",
        "evil corp": "crew",
        "fbl": "militia",
        "fertitta organization": "crew",
        "four brothers": "triad",
        "fratuzzi": "mafia",
        "free women's units of star": "militia",
        "french connection": "cartel",
        "fujita-gumi": "gang",
        "gcp": "militia",
        "george chung's organization": "crew",
        "georgian organized crime": "mafia",
        "german american bund": "faction",
        "ghee hin": "triad",
        "goda-ikka": "mafia",
        "gosha-kai": "faction",
        "goto-gumi": "mafia",
        "grewal/dhaliwal crime group": "gang",
        "guang ji": "triad",
        "gōda-ikka": "mafia",
        "harlem underworld": "faction",
        "hats": "gang",
        "hayashi's yakuza": "mafia",
        "heaven and earth society": "triad",
        "hell's kitchen irish mob": "crew",
        "hemispheric drug syndicates": "cartel",
        "hillbilly bitches": "gang",
        "holmblaaa": "gang",
        "hong pang group": "cartel",
        "hongmen": "triad",
        "hop sing tong": "triad",
        "huallaga regional committee": "terrorist_organization",
        "hung cuba's group": "gang",
        "hung society": "triad",
        "hunt crime syndicate": "crew",
        "hải phòng criminal underworld": "faction",
        "ikeda-gumi": "mafia",
        "ikeshita-gumi": "mafia",
        "imperial japanese yakuza": "mafia",
        "inagawa-kai": "mafia",
        "jabber zeus": "crew",
        "jack spot organization": "crew",
        "jie ji": "triad",
        "jump out boys": "gang",
        "junior business boys": "gang",
        "kagotora-gumi": "mafia",
        "kansai hatsuka-kai": "faction",
        "kanto hatsuka-kai": "faction",
        "kantō hatsuka-kai": "faction",
        "karen national union": "militia",
        "karenni army": "militia",
        "karenni people's defence force": "militia",
        "katiara": "faction",
        "kazakh bratva": "mafia",
        "keiji union": "mafia",
        "kizuna-kai": "mafia",
        "kodo-kai": "faction",
        "kokang force": "militia",
        "kokusui-kai": "mafia",
        "kudo-kai": "mafia",
        "kuki national army": "militia",
        "kyodo-kai": "mafia",
        "kyokuryu-kai": "mafia",
        "kyokushinrengo-kai": "mafia",
        "kyokuto-kai": "mafia",
        "kyosei-kai": "mafia",
        "kyoyu-kai": "mafia",
        "kyushu seido-kai": "mafia",
        "la santa": "faction",
        "lahu democratic union": "militia",
        "lapsus$": "crew",
        "las chemas": "crew",
        "latin souls": "gang",
        "lazarus group": "crew",
        "little devils": "gang",
        "little locos": "gang",
        "lockbit": "crew",
        "locos 13 alliance": "faction",
        "los ardillos": "cartel",
        "los desmadrosos": "gang",
        "los extraditables": "faction",
        "los halcones": "militia",
        "los machos": "faction",
        "los originales": "gang",
        "los palillos": "faction",
        "los perrones": "cartel",
        "los priscos": "faction",
        "lower east side of detroit club": "crew",
        "lyuberetskaya bratva": "mafia",
        "mala del brenta": "mafia",
        "malyshev organization": "mafia",
        "malyshevskaya ogg": "mafia",
        "mano negra": "militia",
        "masaki-gumi": "mafia",
        "matsuba-kai": "mafia",
        "maze": "crew",
        "messina brothers": "crew",
        "miami boys": "gang",
        "midget locos": "gang",
        "midgets": "gang",
        "mora_001": "crew",
        "moston rats": "gang",
        "movimento integralista e linearista brasileiro": "faction",
        "namikawa-kai": "mafia",
        "national liberation front of kurdistan": "faction",
        "national socialist council of nagaland": "militia",
        "new mon state party": "militia",
        "new organized camorra": "mafia",
        "niggaz, honks, chinks": "gang",
        "ninevites": "gang",
        "nishida-gumi": "mafia",
        "nishinippon hatsuka-kai": "faction",
        "noom suk harn": "militia",
        "nova okaida": "faction",
        "okinawa kyokuryu-kai": "mafia",
        "order of the blood": "gang",
        "organization of external security": "terrorist_organization",
        "pa-o national liberation army": "militia",
        "patriot": "militia",
        "pee wee locos": "gang",
        "people's defence force": "militia",
        "people's defence forces": "militia",
        "people's liberation army": "militia",
        "people's liberation army of kurdistan": "militia",
        "peoples' aman committee": "gang",
        "perri-starkman group": "crew",
        "popular nationalist insurgency command of the large brazilian integralist family": "terrorist_organization",
        "primeiro comando de vitória": "faction",
        "primeiro comando puro": "faction",
        "principi group": "gang",
        "ramirez abadía organization": "cartel",
        "ransomhub": "crew",
        "red flag communist party": "militia",
        "red shirts": "militia",
        "redut": "militia",
        "regulators": "gang",
        "revil": "crew",
        "rosa dei venti": "mafia",
        "rusich": "militia",
        "russian bratva": "mafia",
        "sagansky gambling syndicate": "crew",
        "sangre nueva zeta": "faction",
        "sasada-ikka": "mafia",
        "scattered spider": "crew",
        "sekine-gumi": "mafia",
        "sever": "militia",
        "shadow brokers": "crew",
        "shan national united front": "militia",
        "shan state army – north": "militia",
        "shan state army – south": "militia",
        "shan state independence army": "militia",
        "shan united revolutionary army": "militia",
        "shang kal": "mafia",
        "sheng ji": "triad",
        "shin majak": "mafia",
        "shinwa-kai": "mafia",
        "shinyhunters": "crew",
        "sindicato do crime do rio grande do norte": "faction",
        "soai-kai": "mafia",
        "solid wood soldiers": "gang",
        "spagnoli": "mafia",
        "steamers": "gang",
        "sumiyoshi-kai": "mafia",
        "surrendered ulfa": "militia",
        "taishu-kai": "mafia",
        "takumi-gumi": "mafia",
        "tankai-ikka": "mafia",
        "terceiro comando da capital": "faction",
        "the black eagles": "militia",
        "the com": "crew",
        "the golden triangle alliance": "cartel",
        "thomas mcgraw syndicate": "crew",
        "toa-kai": "mafia",
        "tokong society": "triad",
        "tonton macoute": "militia",
        "torina drug ring": "crew",
        "torrio-yale organization": "mafia",
        "tosei-kai": "mafia",
        "trem bala": "faction",
        "tren del llano": "gang",
        "tri-city skins": "gang",
        "tropa de sete": "faction",
        "tung fung benevolent association": "triad",
        "unc2165": "crew",
        "unit 180": "crew",
        "united bamboo": "triad",
        "urías rondón front": "faction",
        "velikie luki": "faction",
        "viv ansanm": "faction",
        "wah kee": "triad",
        "wah sang society": "triad",
        "white lotus society": "triad",
        "white wolves": "terrorist_organization",
        "williams syndicate": "crew",
        "yamaken-gumi": "mafia",
        "yardie": "gang",
        "yondaime manabe-gumi": "mafia",
        "yonsha-kai": "faction",
        "yoshimi-kogyo": "mafia",
        "young boys incorporated": "gang",
        "young munichs": "gang",
        "yug": "militia",
        "zanla": "militia",
        "zeev rosenstein syndicate": "crew",
        "zhang zuolin's forces": "militia",
        "zomi revolutionary army": "militia",
        "abbotsford east asian crime groups": "gang",
        "abdussalam kidnap-for-ransom group": "gang",
        "acdegam": "faction",
        "ahbash": "faction",
        "aizukotetsu-kai": "mafia",
        "akira": "crew",
        "alema leota's samoan mob": "crew",
        "alianza fronteriza": "cartel",
        "alphv": "crew",
        "american lebanese club": "crew",
        "amir molnar syndicate": "crew",
        "andariel": "crew",
        "anqing daoyou": "gang",
        "arkan network": "crew",
        "arkan's syndicate": "crew",
        "ashkenazum": "faction",
        "asociación campesina de ganaderos y agricultores del magdalena medio": "faction",
        "australian syndicates": "crew",
        "balkan express": "crew",
        "bandogs": "gang",
        "baron criminal group": "gang",
        "bauman organized crime group": "mafia",
        "benson syndicate": "crew",
        "beomseobangpa": "mafia",
        "betawi brotherhood forum": "faction",
        "bicheiros": "faction",
        "black b. inc.": "faction",
        "camera di controllo": "faction",
        "campeatori": "gang",
        "cappiatori": "gang",
        "chaozhou syndicates": "triad",
        "charles solomon organization": "crew",
        "charlie wall's organization": "crew",
        "chee kung tong": "triad",
        "chicago south club": "crew",
        "chilsung-pa": "mafia",
        "chong-ro": "gang",
        "church of jesus christ–christian": "faction",
        "compagniani": "gang",
        "council of eight": "faction",
        "dojin-kai": "mafia",
        "duhre group of british columbia": "gang",
        "federation of the world order in europe": "faction",
        "federation of turkish democratic idealist associations in germany": "faction",
        "frente integralista brasileira": "faction",
        "fukuhaku-kai": "mafia",
        "irish mob in new bordeaux": "crew",
        "irish-american organized crime": "faction",
        "iron guard of egypt": "faction",
        "islamic courts union": "faction",
        "italian-american crime syndicates": "mafia",
        "jewish crime syndicates": "faction",
        "kaabouni organisation": "cartel",
        "karaburma group": "crew",
        "keka group": "crew",
        "khánh trắng's organization": "crew",
        "kommando 210": "gang",
        "korean kkangpae": "gang",
        "krieger verwandt": "gang",
        "kurdistan communities union": "faction",
        "kurdistan freedom brigades": "militia",
        "lab 110": "crew",
        "lambri pirates": "crew",
        "latin american defense organization": "faction",
        "lealtà e azione": "gang",
        "long ya men pirates": "crew",
        "los paisas": "militia",
        "los puntilleros": "militia",
        "luojiao": "faction",
        "lê ngọc lâm's organization": "crew",
        "m58 firm": "gang",
        "makoto-kai": "faction",
        "maximus ocg": "cartel",
        "megpunna thugs": "gang",
        "mercado do povo atitude": "faction",
        "meyer lansky's criminal enterprises": "crew",
        "moltanee thugs": "gang",
        "mom and pop coyote businesses": "crew",
        "montreal underworld": "faction",
        "morena": "faction",
        "moroccan drug traffickers": "cartel",
        "movement pro independence": "faction",
        "mujahideen commanders": "militia",
        "mumbai underworld": "faction",
        "mungiki": "gang",
        "myung-dong": "gang",
        "neapolitan camorristi": "gang",
        "neo black movement of africa": "gang",
        "new belgrade malls groups": "gang",
        "new illegal armed groups": "militia",
        "new world of islam": "faction",
        "nicaraguan drug traffickers": "cartel",
        "nigerian crime syndicates": "faction",
        "non-state groups in kashmir": "militia",
        "northern alliance": "militia",
        "northern moroccan hash kingpins": "cartel",
        "nueva generación": "militia",
        "outfit": "mafia",
        "pascuzzi combine": "faction",
        "patriot party (pemuda pancasila)": "faction",
        "patriotic revolutionary youth movement": "faction",
        "pedro león arboleda movement": "militia",
        "pendergast machine": "faction",
        "peruvian communist party": "faction",
        "peruvian communist party – red flag": "faction",
        "phase iii smuggling organization": "cartel",
        "pindari bands": "militia",
        "piracy organizations": "crew",
        "puerto rican revolutionary workers organization": "faction",
        "pyrates": "gang",
        "racial volunteer force": "terrorist_organization",
        "raiders organization": "faction",
        "rathkeale rovers": "clan",
        "renacer": "militia",
        "revolutionary internationalist movement": "faction",
        "river thugs": "gang",
        "romanian groups": "gang",
        "sam maceo's organization": "crew",
        "samoan criminal outfits": "gang",
        "sarasota assassination society": "faction",
        "scandinaviancarding": "crew",
        "seattle's crime network": "faction",
        "senjak group": "crew",
        "serbian brotherhood": "crew",
        "simón bolívar guerrilla coordinating board": "faction",
        "sindouse thugs": "gang",
        "society for common progress": "faction",
        "somali drug trafficking groups": "gang",
        "somali pirate groups": "crew",
        "somali pirates": "crew",
        "soosea thugs": "gang",
        "southeast asian organized crime networks": "cartel",
        "southern moroccan traffickers": "cartel",
        "spanish garduña": "faction",
        "spanish growth and development": "faction",
        "spatola-inzerillo-gambino network": "faction",
        "sudanese drug trafficking groups": "gang",
        "suki": "faction",
        "swedish grey wolves": "faction",
        "taiwanese criminal syndicates": "gang",
        "tashma-baz thugs": "gang",
        "telingana thugs": "gang",
        "the cleaners": "crew",
        "the company": "crew",
        "the corporation": "cartel",
        "the creativity movement": "faction",
        "thugs": "gang",
        "tokuryū": "gang",
        "tongs": "triad",
        "tropa del infierno": "faction",
        "turkish cypriot crime groups": "crew",
        "turkish federation netherlands": "faction",
        "turkish islamic federation": "faction",
        "ukrainian criminal group in poland": "crew",
        "union of turkish-islamic cultural associations in europe": "faction",
        "united front and people's guerrilla army": "terrorist_organization",
        "uzbek criminals network": "cartel",
        "veljko belivuk criminal group": "crew",
        "vietnamese groups": "gang",
        "wallace organisation": "crew",
        "white brotherhood": "cartel",
        "zemunska / bojović ocg": "clan",
        "zero network": "gang",
        "zwi migdal": "faction",
        "detroit federation of motorcycle clubs": "motorcycle_club",
        "dev-genç": "militia",
        "dutch antillean criminal organizations": "gang",
        "dutch penose": "mafia",
        "dzambo dzambidze organized crime group": "mafia",
        "ethiopian drug trafficking groups": "gang",
        "executeurs": "gang",
        "federaciones cocaleras del trópico": "cartel",
        "federación especial de colonizadores de chimoré": "cartel",
        "front pembela islam": "militia",
        "garduña": "mafia",
        "gargano group": "clan",
        "goan drug trade groups": "cartel",
        "human smuggling organizations": "cartel",
        "independent traffickers": "cartel",
        "indian thuggee": "gang",
        "indian-origin crime groups in canada and uk": "gang",
        "indonesian preman": "gang",
        "irish alliance": "motorcycle_club",
        "irish northern aid committee": "faction",
        "israeli crime figures": "mafia",
        "jago": "gang",
        "kazan phenomenon": "gang",
        "kintex": "cartel",
        "lebanese organized crime groups": "mafia",
        "macanese organized crime groups": "triad",
        "ming syndicate": "gang",
        "montreal-based organized crime group": "mafia",
        "mulner organization": "gang",
        "muloc group": "gang",
        "multigroup": "mafia",
        "nagazi": "mafia",
        "native hawaiian crime syndicates": "gang",
        "neo-nazi and nationalist groups": "gang",
        "nigerian organized crime groups": "mafia",
        "obshchak": "mafia",
        "organised crime syndicates": "gang",
        "organized crime in southeast asia": "gang",
        "os do barbanza": "clan",
        "paulino castillo organization": "cartel",
        "phase ii smuggling organization": "cartel",
        "section organizations": "gang",
        "traditional organized crime groups": "mafia",
        "trans-america":     "crew",
        "uzbek network":     "cartel",
        "los ántrax":        "cartel", # bodyguard squadron OF the Sinaloa Cartel
        "machos":            "cartel", # private army within Norte del Valle Cartel
        "kuratong baleleng": "gang",   # Philippine crime syndicate: kidnapping, drugs, extortion
        "bình xuyên":        "gang",   # ran brothels, casinos, protection rackets in Saigon
        "gente nueva": "cartel",
        "los ántrax": "cartel",
        "los matazetas": "cartel",
        "los puntilleros": "cartel",
        "los paisas": "cartel",
        "nueva generación": "cartel",
        "renacer": "cartel",
        "erpac": "cartel",
        "mano negra": "cartel",
        "machos": "cartel",
        "pueblos unidos": "cartel",
        "border command": "cartel",
        "carolina ramirez front": "cartel",
        "black eagles": "cartel",
        "the black eagles": "cartel",
        "bloque centauros": "cartel",
        "bloque meta": "cartel",
        "bloque elmer cardenas": "cartel",
        "autodefensas unidas de colombia": "cartel",
        "paisas": "gang",
        "kuratong baleleng": "gang",
        "bình xuyên": "gang",
        "mkhedrioni": "gang",
        "jso": "gang",
        "escritório do crime": "gang",
        "axe gang": "gang",
        "united klans of america": "gang",
        "white patriot party": "gang",
        "ejército popular de liberación": "militia",
        "pancasila youth": "faction",
        "rise above movement": "faction",
        "lealtà e azione": "faction",
        "indonesian preman": "faction",
        "preman": "faction",
        "jago": "faction",
        # --- CARTEL → more accurate type ---
        "irish mob": "mafia",                      # Irish organized crime = mafia, not cartel
        "liverpool mafia": "mafia",                # it's literally called mafia
        "vancouver mob": "mafia",                  # organized crime group = mafia
        "marielitos": "gang",                      # Cuban crime groups, gang-level
        # --- GANG → more accurate type ---
        "aryan nations": "faction",                # political/hate organization, not a street gang
        "chao pho": "mafia",                       # Thai organized crime bosses/godfathers = mafia
        "honghuzi": "militia",                     # Chinese bandits/guerrilla fighters, historical paramilitary
        "mainland chinese gang": "gang",           # ok as gang but very vague
        "campeatori": "gang",                      # ok but historical Naples proto-Camorra
        "cappiatori": "gang",                      # ok but historical Naples proto-Camorra
        "compagniani": "gang",                     # ok but historical Naples proto-Camorra — these 3 might be better as faction (proto-Camorra)
        # --- FACTION → more accurate type ---
        "manson family": "gang",                   # criminal cult/gang, not a "faction" of anything
        "betawi brotherhood forum": "gang",        # Indonesian preman gang org

        # --- MILITIA (political-military movements, territorial control, de facto governance) ---
        "houthi movement": "militia",                   # political-military movement, de facto government in Yemen; only some countries designate as terrorist
        "hizbul islam": "militia",                      # Somali Islamist militia, merged with Al-Shabaab; functioned as armed militia
        "azerbaijani grey wolves": "militia",           # nationalist paramilitary org, not widely designated as terrorist
        "khmer rouge": "militia",                       # political party/regime that committed genocide — was a government, not a "terrorist org"

        # --- GANG (criminal enterprises, robberies, skinhead networks) ---
        "aryan republican army": "gang",                # self-described as a gang; robbed 22 banks to fund white supremacist movement
        "the order": "gang",                            # neo-Nazi robbery/murder group (Brüder Schweigen)
        "the order ii": "gang",                         # successor gang, bombings in Idaho
        "the new order": "gang",                        # inspired by The Order, arrested before attacks
        "hammerskins": "gang",                          # neo-Nazi skinhead gang network
        "blood & honour": "gang",                       # neo-Nazi skinhead network, functions as gang
        "blood & honour australia": "gang",             # Australian chapter, same nature
        "finnish blood & honour": "gang",               # Finnish chapter, same nature
        "keystone united": "gang",                      # neo-Nazi skinhead gang, violent assaults
        "volksfront": "gang",                           # neo-Nazi skinhead gang
        "white aryan resistance": "gang",               # hate group with gang characteristics, members joined Bandidos
        "white wolves": "gang",                         # Combat 18 splinter, small gang
        "racial volunteer force": "gang",               # Combat 18 splinter, small gang
        "special purpose islamic regiment": "gang",     # Chechen group primarily involved in kidnapping and criminal fundraising
        "symbionese liberation army": "gang",           # tiny armed group (6-12 members), kidnapping/robbery
        "republican action against drugs": "gang",      # vigilante gang in Derry, punishment beatings/killings

        # --- FACTION (cover names, splinters, political sub-groups, not independent orgs) ---
        "direct action against drugs": "faction",       # IRA cover name, not an independent organization
        "republican action force": "faction",           # IRA cover name for sectarian attacks
        "ajang ajang group": "faction",                 # cell within Abu Sayyaf, not independent
        "huallaga regional committee": "faction",       # Shining Path regional committee, not independent org
        "sendero rojo": "faction",                      # Shining Path faction
        "proseguir": "faction",                         # Shining Path faction
        "fajr organization": "faction",                 # Hezbollah front/cover name
        "khaybar brigade": "faction",                   # Hezbollah front/cover name
        "followers of the prophet muhammad": "faction", # Hezbollah front/cover name
        "organization of right against wrong": "faction",       # Hezbollah front
        "organization of the oppressed": "faction",             # Hezbollah front
        "organization of the world's oppressed": "faction",     # Hezbollah front
        "organization of islamic jihad for the liberation of palestine": "faction",  # Hezbollah front
        "revolutionary justice organization": "faction",        # Hezbollah front
        "organization of external security": "faction",         # Hezbollah operational unit
        "crusaders of yahweh": "faction",               # Aryan Nations splinter
        "aryan nations revival": "faction",             # Aryan Nations splinter
        "holy order of the brotherhood of the phineas priesthood": "faction",  # Aryan Nations enforcement wing
        "popular nationalist insurgency command of the large brazilian integralist family": "faction",  # tiny Brazilian far-right splinter, one-off incident
        "estado mayor central": "faction",              # FARC dissident faction
        "farc dissidents": "faction",                   # FARC splinter groups
        "farc-ep dissidents": "faction",                # same, duplicate label
        "volunteers": "faction",                        # operated with Boricua Popular Army, not independent
        # add more here...
    }

    for node in nodes:
        override = NODE_TYPE_OVERRIDES.get(node["standard_name"].strip().lower())
        if override:
            node["type"] = override

    for edge in edges:
        edge["source"] = merge_map.get(edge["source"], edge["source"])
        edge["target"] = merge_map.get(edge["target"], edge["target"])

    edges = [e for e in edges if normalize(e["source"]) != normalize(e["target"])]

    edge_map = {}
    for edge in edges:
        key = (normalize(edge["source"]), normalize(edge["target"]),
               edge.get("relationship", ""), edge.get("detail") or "")
        if key not in edge_map:
            edge_map[key] = edge
        else:
            existing = edge_map[key]
            if len(edge.get("context", "")) > len(existing.get("context", "")):
                existing["context"] = edge["context"]
            if len(edge.get("time_period") or "") > len(existing.get("time_period") or ""):
                existing["time_period"] = edge["time_period"]
            existing["wikipedia_urls"] = sorted(
                set(existing.get("wikipedia_urls", [])) | set(edge.get("wikipedia_urls", []))
            )
    edges = list(edge_map.values())

    # 4. URL sanitization
    bad = 0
    for node in nodes:
        orig = node.get("wikipedia_urls", [])
        clean = [u for u in orig if is_valid_wiki_url(u)]
        if len(clean) < len(orig):
            bad += len(orig) - len(clean)
        node["wikipedia_urls"] = clean
    for edge in edges:
        orig = edge.get("wikipedia_urls", [])
        clean = [u for u in orig if is_valid_wiki_url(u)]
        if len(clean) < len(orig):
            bad += len(orig) - len(clean)
        edge["wikipedia_urls"] = clean
    if bad:
        log.info(f"URL sanitization: removed {bad} broken URLs")

    log.info(f"After cleanup: {len(nodes)} nodes, {len(edges)} edges")

    if show_stats:
        tc = Counter(n["type"] for n in nodes)
        rc = Counter(e["relationship"] for e in edges)
        dc = Counter(e["detail"] for e in edges if e.get("detail"))
        print(f"\n{'='*60}\n  CLEANED NETWORK\n{'='*60}")
        print(f"  Nodes: {len(nodes)}  |  Edges: {len(edges)}\n")
        print(f"  Node types ({len(tc)}):")
        for t, c in tc.most_common():
            print(f"    {t:35s} {c:5d}")
        print(f"\n  Relationships:")
        for t, c in rc.most_common():
            print(f"    {t:35s} {c:5d}")
        if dc:
            print(f"\n  Detail types ({len(dc)}):")
            for t, c in dc.most_common():
                print(f"    {t:35s} {c:5d}")
        print("=" * 60)

    return nodes, edges


# ═══════════════════════════════════════════════════════════════════
# BUILD crimenet_specific.json
# ═══════════════════════════════════════════════════════════════════

SPECIFIC_ORG_TYPES = CANONICAL_NODE_TYPES  # all canonical types count

SPECIFIC_EDGE_MAP = {"alliance": "allied_with", "rivalry": "rivals_with"}


def build_specific(nodes, edges):
    org_names = set()
    entities = []
    filtered = []

    for n in nodes:
        if n["type"] not in SPECIFIC_ORG_TYPES:
            continue
        if is_generic_node(n["standard_name"]):
            filtered.append(n["standard_name"])
            continue
        org_names.add(n["standard_name"])
        own_src, mentioned = split_node_sources(
            n["standard_name"], n.get("aliases", []), n.get("wikipedia_urls", []),
        )
        entities.append({
            "name": n["standard_name"],
            "type": n["type"],
            "descriptions": [n["context"]] if n.get("context") else [],
            "time_period": n.get("time_period") or None,
            "own_source": own_src,
            "mentioned_in": mentioned,
        })

    if filtered:
        log.info(f"Filtered {len(filtered)} generic nodes:")
        for name in sorted(filtered):
            log.info(f"    ✗ {name}")

    spec_edges = [e for e in edges
                  if e["relationship"] in SPECIFIC_EDGE_MAP
                  and e["source"] in org_names and e["target"] in org_names]

    # Separate edges by type
    alliance_edges = [e for e in spec_edges if e["relationship"] == "alliance"]
    rivalry_edges = [e for e in spec_edges if e["relationship"] == "rivalry"]

    # Compute betweenness for each view
    log.info("Computing betweenness (alliance only):")
    bc_alliance = compute_betweenness(org_names, alliance_edges)
    log.info("Computing betweenness (rivalry only):")
    bc_rivalry = compute_betweenness(org_names, rivalry_edges)
    log.info("Computing betweenness (combined):")
    bc_combined = compute_betweenness(org_names, spec_edges)

    for ent in entities:
        name = ent["name"]
        ent["betweenness_alliance"] = round(bc_alliance.get(name, 0.0), 6)
        ent["betweenness_rivalry"] = round(bc_rivalry.get(name, 0.0), 6)
        ent["betweenness_combined"] = round(bc_combined.get(name, 0.0), 6)

    relations = []
    for e in spec_edges:
        relations.append({
            "source": e["source"],
            "target": e["target"],
            "type": SPECIFIC_EDGE_MAP[e["relationship"]],
            "descriptions": [e["context"]] if e.get("context") else [],
            "time_period": e.get("time_period") or None,
            "sources": [
                {"url": url, "title": extract_wiki_title(url) or "Wikipedia"}
                for url in e.get("wikipedia_urls", [])
            ],
        })

    return {"entities": entities, "relations": relations}


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Cleanup and build crimenet_specific.json")
    parser.add_argument("--input", "-i", default="global_network.json")
    parser.add_argument("--stats", "-s", action="store_true")
    args = parser.parse_args()

    data = json.loads(Path(args.input).read_text("utf-8"))
    nodes, edges = cleanup(data, show_stats=args.stats)

    specific = build_specific(nodes, edges)
    Path("crimenet_specific.json").write_text(
        json.dumps(specific, ensure_ascii=False, indent=2), "utf-8"
    )
    log.info(f"Output: {len(specific['entities'])} entities, {len(specific['relations'])} relations → crimenet_specific.json")


if __name__ == "__main__":
    main()