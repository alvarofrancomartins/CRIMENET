"""
curated_corrections.py — all hand-curated corrections.

EDIT THIS FILE BY HAND: paste the good suggestions from audit/audit_data/
into the dicts below, then run 7_apply_corrections.py to rebuild crimenet.json.

Imported by audit/7_apply_corrections.py, which reads crimenet_raw.json
and applies every correction here to produce crimenet.json.

Keys are lowercased on use, so put entries in whatever case is most
readable. The matching is always case-insensitive.
"""

# ═══════════════════════════════════════════════════════════════════
# KNOWN_DUPLICATES
# Maps a canonical name to a list of variant spellings the LLM emits.
# All matching is case-insensitive.
# ═══════════════════════════════════════════════════════════════════
KNOWN_DUPLICATES = {
    # ── Italy ────────────────────────────────────────────────────────
    "'Ndrangheta": {
        "'Ndrangheta di San Luca", "'Ndrangheta di Cosenza", 
        "'Ndrangheta clans of Platì", "'Ndrangheta clans of Cosenza", 
        "'Ndrangheta di Platì", "'Ndrangheta clans of Crotone", 
        "'Ndrangheta clans of San Luca", "'Ndrangheta di Crotone"
    }, # Keeps geographical additions from splitting the main node
    "Šarić clan": {"Šarić gang"}, # 'gang' is not stripped by generic tokens
    "Kinahan Organised Crime Group": {"Kinahan clan"},

    # ── South America ────────────────────────────────────────────────
    "Amigos dos Amigos": {"ADA"},
    "Bala na Cara": {"BNC"},
    "Bonde dos 13": {"B13"},
    "Comando Vermelho": {"Red Command", "CV"},
    "Família Terror de Amapá": {"Família Terror", "Família Terror do Amapá", "FTA"},
    "Terceiro Comando Puro": {"TCP"},

    # ── Central / North America ──────────────────────────────────────
    "Jalisco New Generation Cartel": {
        "Jalisco Cartel", "New Generation Cartel", "Jalisco New Generation", 
        "CJNG", "Cártel de Jalisco Nueva Generación"
    },

    # ── International / Other ────────────────────────────────────────
    "Ang Soon Tog": {"Ang Soon Toog"}, # Typo correction
    "Badr Organization": {"Badr Brigade"},
    "Black Power (New Zealand gang)": {"Black Power"},
    "Los Bravos MC": {"Los Bravos", "Los Brovos"}, # Typo correction
    "United Nations (gang)": {"United Nations"},

    "Triads": {"Chinese Triads", "Triad"},
    "14K": {"14K Triad"},
    "American Mafia": {"American Cosa Nostra"},
    "Casalesi clan": {"Clan dei Casalesi", "Bardellino clan", "Camorra Casalesi clan"},
    "Commisso 'ndrina": {"'Ndrina Macrì", "'Ndrina Commisso"},
    "'Ndrina Tegano": {"'Ndrina De Stefano"},

    "'Ndrina Bruzzise": {"'Ndrina Bruzzese"},

    "La Familia Michoacana": {"La Familia Cartel"},

    "La Raza Nation": {"La Raza"},

    "'Ndrina Morabito": {"Morabito", "Morabito 'ndrina"},

    "Playboys 13 Gang": {"Playboys 13", "Playboys"},
    
    # The en_Đại_Cathay article was declined as a person by step 3, so no
    # "Đại Cathay organization" node exists. Fold the mention nodes under the
    # real node "Đại Cathay" (NODE_OVERRIDES below gives it a full profile).
    "Đại Cathay": {"Đại Cathay's mob", "Đại Cathay's gang", "Đại Cathay organization"},

    "East Side Longos": {"East Side Longo"},

    "Sin Ma gang": {"Sin Ma"},

    "Wild Ones Motorcycle Club": {"Wild Ones"},

    # High-Degree Unifications
    "Milenio Cartel": {"Cártel del Milenio"},
    "'Ndrina Mancuso": {"Mancuso 'ndrina"},
    "Mazzarella clan": {"Clan Mazzarella", "Mazzarella"},
    "Cartel of the Suns": {"Cartel de los Soles"},
    "'Ndrina Barbaro": {"Barbaro 'ndrina"},
    "'Ndrina Pelle": {"Pelle 'ndrina"},
    
    # Mid-Degree Suffix/Inversion Groups
    "De Luca Bossa clan": {"Clan De Luca Bossa"},
    "Cesarano clan": {"Clan Cesarano"},
    "Sinesi-Francavilla": {"Clan Sinesi-Francavilla"},
    "Nuvoletta clan": {"Clan Nuvoletta"},
    "Zaza clan": {"Clan Zaza"},
    "Anastasio clan": {"Clan Anastasio"},
    "Nueva Plaza Cartel": {"Cártel Nueva Plaza"},
    "Chee Kung Tong": {"Ghee Kung Tong"},
    
    # Low-Degree Structural Splits
    "Clan Marfella": {"Marfella clan"},
    "Amirante clan": {"Clan Amirante"},
    "'Ndrina Tripodo": {"Tripodo", "Tripodo 'ndrina"},
    "Dung Hà's Gang": {"Dung Hà gang"},
    "Tín Mã Nàm": {"Tín Mã Nàm's gang"},
    "Molluso 'ndrina": {"'Ndrina Molluso"},
    "Zappia 'ndrina": {"'Ndrina Zappia"},
    "Campanella Park Piru": {"Campanella Park Pirus"},

    # High-Degree Consolidated Targets
    "Popular Liberation Army": {"Ejército Popular de Liberación"},
    "'Ndrina Piromalli": {"Piromalli 'ndrina"},
    "Clan Alfieri": {"Alfieri clan"},
    "Moccia clan": {"Clan Moccia"},
    "Dhak-Duhre group": {"Dhak crime group", "Dhak-Duhre gang"},  # 'gang' and 'group' are the same Indo-Canadian org (one Alkhalil-family article)
    "Licciardi clan": {"Clan Licciardi"},
}

# ═══════════════════════════════════════════════════════════════════
# NODE_OVERRIDES
# Manually forces specific fields on a resolved node. Useful for 
# fixing contaminated 'is_defunct' statuses
# or explicitly defining the 'own_sources' to drop noisy sub-articles.
# ═══════════════════════════════════════════════════════════════════
NODE_OVERRIDES = {
    "hells angels motorcycle club": {
        "is_defunct": "false",
        "dissolved_year": "null",
        "time_period": "1948-present",
        "own_sources": [
            "https://en.wikipedia.org/w/index.php?title=Hells_Angels&oldid=1351206856"
        ]
    },

    # The EN La Resistencia article says "As of 2023 ... the group is now
    # disbanded", but Cárteles Unidos is active. Override the bad defunct flag.
    "Cárteles Unidos": {
        "is_defunct": "false",
        "dissolved_year": "null",
    },

    # Stale time_period: each of these has a real dissolved_year but the merged
    # time_period still ends in "-present". Step 4 keeps the LONGEST period
    # string across language articles, so a still-"present" span from one
    # article survives even when another records the dissolution (e.g. the PT
    # Família do Norte / Guardiões do Estado pages give the end, the EN ones do
    # not). End the span at the dissolved year and lock is_defunct=true. See the
    # cross-language divergence note in the README.
    "Família do Norte":       {"is_defunct": "true", "time_period": "2007-2021"},
    "Guardiões do Estado":    {"is_defunct": "true", "time_period": "2017-2025"},
    "Knights Templar Cartel": {"is_defunct": "true", "time_period": "December 2010-2017"},
    "Los Zetas":              {"is_defunct": "true", "time_period": "1997-2018"},
    "Loyal to Familia":       {"is_defunct": "true", "time_period": "2013-2021"},
    "Zemun Clan":             {"is_defunct": "true", "time_period": "20th century-2003"},

    "Đại Cathay": {
        "description": "Đại Cathay, whose real name was Lê Văn Đại, was a 1960s Saigon mobster, and was considered as the top of the 'four great kings' of Saigon's criminal underworld before 1975, followed by Huỳnh Tỳ, Ngô Văn Cái and Ba Thế.",
        "is_defunct": "true",
        "founded_year": "1960s",
        "time_period": "1960s-1966",
        "dissolved_year": "1966",
        "country": "Vietnam",
        "own_sources": [
            "https://en.wikipedia.org/w/index.php?title=%C4%90%E1%BA%A1i_Cathay&oldid=1337702587"
        ]
        
        }
    }

# ═══════════════════════════════════════════════════════════════════
# BLOCKLIST
# "These names must NEVER be merged into the same node — by any
# mechanism (exact, alias, or fuzzy core matching)."
#
# Format: { name: {names it must never merge with} }, case-insensitive.
# The relationship is treated as SYMMETRIC — order doesn't matter, and
# you only need to write each pair once.
#
# This one structure covers every kind of wrong merge:
#
#   • Alias collision — a name that is a legitimate alias of one org but
#     ALSO the name of a different org:
#         "Al-Qaeda": {"the Base"}
#     ("the Base" is Al-Qaeda's alias but also a neo-Nazi group; this
#      stops the neo-Nazi node folding into Al-Qaeda.)
#
#   • Fuzzy core-token collision — two distinct orgs that reduce to the
#     same significant token after generic words are stripped:
#         "Native Syndicate": {"Native Mob"}
#     ("syndicate"/"mob" are stripped, both → {native}; this keeps the
#      two distinct Indigenous gangs separate.)
#
# To find which name to block against, run audit/0_review_wrong_merges.py: it lists every
# name folded into each canonical (with ⚠ via-alias / [core] risk flags) and
# has DeepSeek flag the wrong ones. Block a name against whatever canonical it
# wrongly resolved to.
# ═══════════════════════════════════════════════════════════════════
BLOCKLIST = {
    "Al-Qaeda": {"the Base"},
    "Native Syndicate": {"Native Mob"},
    "Cleveland Syndicate": {"Cleveland crime family"},
    "'Ndrangheta": {"Mano Nera", "Black Hand"},
    "American Mafia": {"Cosa Nostra", "La Cosa Nostra", "LCN", "Sicilian Cosa Nostra", "Italian Mob", "Mafia"},

    # ── Distinct orgs that share a name (manual review 2026-06-14) ──
    # (Los Zetas vs its splinters is folded into the annotated "Los Zetas" entry below.)
    "Cártel de los Rojos": {"Los Rojos"},
    "East Harlem Purple Gang": {"Purple Gang"},
    "Forty Elephants": {"Forty Thieves"},
    # "The Company" / "Sam Gor" / "Ah Kong" are already separate nodes in raw.
    # "Sacra corona unita" / "Società foggiana" / "Nuova camorra pugliese" are
    # already separate nodes in raw.  Do NOT add blocklist entries here when
    # the names only meet through an alias shared by different nodes — the
    # apply_blocklist function finds the alias inside one node and renames it,
    # producing duplicate standard_name collisions.

    # 'Ndrina Arena: https://it.wikipedia.org/w/index.php?title=%27Ndrina_Arena&oldid=150106900
    "'Ndrina Arena": {
        "Arena 'ndrina",  # (high) Record A describes the Arena clan based in Isola Capo Rizzuto, while Record B describes a different Arena clan originally from San Luca.
        #     ↳ Arena 'ndrina: https://en.wikipedia.org/w/index.php?title=Honoured_Society_%28Australia%29&oldid=1316117128
    },

    # Serb mafia in Scandinavia: https://en.wikipedia.org/w/index.php?title=Serb_mafia_in_Scandinavia&oldid=1339086605
    "Serb mafia in Scandinavia": {
        "Brotherhood Motorcycle Club",  # (high) Record A is a Serb mafia group, while Record B is a motorcycle club; they are different types of organizations.
        #     ↳ Brotherhood Motorcycle Club: https://en.wikipedia.org/w/index.php?title=Hells_Angels_MC_criminal_allegations_and_incidents&oldid=1353872599
        "The Brotherhood",  # (high) Record A is a Serbian mafia group in Scandinavia, while Record B is a UK motorcycle club branch with no connection.
        #     ↳ The Brotherhood: https://en.wikipedia.org/w/index.php?title=Black_Pistons_Motorcycle_Club&oldid=1351074542
    },

        # 'Ndrina Albanese: https://it.wikipedia.org/w/index.php?title=%27Ndrina_Albanese&oldid=149610970
    "'Ndrina Albanese": {
        "Mafia albanese",  # (high) Record A is a specific 'Ndrangheta clan in Italy, while Record B refers to Albanian organized crime groups in Italy, which are distinct entities.
        #     ↳ Mafia albanese: https://it.wikipedia.org/w/index.php?title=Mafia_in_Italia&oldid=150010027
    },
    # # 'Ndrina Carpino: https://it.wikipedia.org/w/index.php?title=%27Ndrina_Carpino&oldid=149622984
    # "'Ndrina Carpino": {
    #     "Carpino",  # (low) Record B's description is too vague to confirm it refers to the same 'Ndrina Carpino; it could be a different entity or a generic reference.
    #     #     ↳ Carpino: https://it.wikipedia.org/w/index.php?title=%27Ndrina_Arena&oldid=150106900
    # },
    # # 'Ndrina Ferraro: https://it.wikipedia.org/w/index.php?title=%27Ndrina_Ferraro&oldid=149623772
    # "'Ndrina Ferraro": {
    #     "'Ndrina Ferraro-Raccosta",  # (high) Record A describes the Ferraro 'ndrina allied with Raccosta, while Record B describes a rival 'ndrina Ferraro-Raccosta in the Oppido feud.
    #     #     ↳ 'Ndrina Ferraro-Raccosta: https://it.wikipedia.org/w/index.php?title=%27Ndrina_Mazzagatti&oldid=149627886
    # },
    # 'Ndrina Gallico: https://it.wikipedia.org/w/index.php?title=%27Ndrina_Gallico&oldid=149189587
    "'Ndrina Gallico": {
        "Gallico clan",  # (high) Record A is a 'Ndrangheta 'ndrina in Calabria, while Record B is a Camorra clan in Campania, distinct mafia organizations.
        #     ↳ Gallico clan: https://it.wikipedia.org/w/index.php?title=Clan_Licciardi&oldid=149575539
    },
    # 'Ndrina La Rosa: https://it.wikipedia.org/w/index.php?title=%27Ndrina_La_Rosa&oldid=149624327
    "'Ndrina La Rosa": {
        "La Rosa",  # (high) Record A is a 'Ndrangheta family in Tropea, Italy, while Record B is a branch of SCU in Bari, Italy, with different locations and time periods.
        #     ↳ La Rosa: https://it.wikipedia.org/w/index.php?title=Sacra_corona_unita&oldid=150547378
    },
    # 'Ndrina Romeo: https://it.wikipedia.org/w/index.php?title=%27Ndrina_Romeo&oldid=149618103
    "'Ndrina Romeo": {
        "Romeo 'ndrina",  # (high) Record A is the Romeo clan based in San Luca, while Record B is a different Romeo clan in Platì associated with the Barbaro family.
        #     ↳ Romeo 'ndrina: https://en.wikipedia.org/w/index.php?title=Francesco_Barbaro_%28Castanu%29&oldid=1341320471
    },
    "'Ndrina Tegano": {
        "De Stefano",  # (high) Ndrina Tegano and De Stefano are distinct 'Ndrangheta clans with different names and histories.
        #     ↳ De Stefano: https://it.wikipedia.org/w/index.php?title=%27Ndrangheta&oldid=150697108
        "De Stefano clan",  # (low) Ndrina Tegano and De Stefano clan are distinct 'Ndrangheta clans; no evidence they are the same.
        #     ↳ De Stefano clan: https://it.wikipedia.org/w/index.php?title=Camorra&oldid=150599702
    },
    # 'Ndrina Violi: https://it.wikipedia.org/w/index.php?title=%27Ndrina_Violi&oldid=149662003
    "'Ndrina Violi": {
        "Violi family",  # (high) Record A is a specific 'ndrina in Italy, while Record B is a Canadian crime family; different locations and structures.
        #     ↳ Violi family: https://en.wikipedia.org/w/index.php?title=%27Ndrangheta&oldid=1353251233
    },
    # # 39ers gang: https://en.wikipedia.org/w/index.php?title=39ers_gang&oldid=1339076334
    # "39ers gang": {
    #     "39ers",  # (low) Record B only mentions an ally relationship with no location or details, insufficient to confirm it's the same New Orleans gang.
    #     #     ↳ 39ers: https://en.wikipedia.org/w/index.php?title=Byrd_Gang&oldid=1339077042
    # },
    # Al-Shabaab: https://en.wikipedia.org/w/index.php?title=Al-Shabaab_%28militant_group%29&oldid=1353959560
    "Al-Shabaab": {
        "Al-Shabaab (Mozambique)",  # (high) Al-Shabaab in Somalia is a distinct al-Qaeda-linked group, while Al-Shabaab in Mozambique is a separate Islamist group that evolved into ISMP.
        #     ↳ Al-Shabaab (Mozambique): https://en.wikipedia.org/w/index.php?title=Islamic_State&oldid=1354566840
    },
    # Artistas Asesinos: https://en.wikipedia.org/w/index.php?title=Artistas_Asesinos&oldid=1339076599 | https://es.wikipedia.org/w/index.php?title=Artistas_Asesinos&oldid=170233205
    "Artistas Asesinos": {
        "Arakan Army",  # (high) Record A is a Mexican street gang, while Record B is a Myanmar insurgent group; they are unrelated.
        #     ↳ Arakan Army: https://en.wikipedia.org/w/index.php?title=Myanmar_conflict&oldid=1350747295
    },
    # Bala na Cara: https://pt.wikipedia.org/w/index.php?title=Bala_na_Cara&oldid=71560602
    "Bala na Cara": {
        "Brasil Nova Cartel",  # (low) Bala na Cara is a well-documented Brazilian gang, while Brasil Nova Cartel is an obscure faction allied with PGC with no clear connection.
        #     ↳ Brasil Nova Cartel: https://pt.wikipedia.org/w/index.php?title=Primeiro_Grupo_Catarinense&oldid=70572643
    },
    # Bandas y grupos emergentes en Colombia: https://es.wikipedia.org/w/index.php?title=Bandas_y_grupos_emergentes_en_Colombia&oldid=172364243
    "Bandas y grupos emergentes en Colombia": {
        "Disidencias de las FARC-EP",  # (high) Record A describes post-paramilitary criminal groups (Bacrim/GAO), while Record B describes FARC dissidents who rejected the peace deal; they are distinct entities.
        #     ↳ Disidencias de las FARC-EP: https://es.wikipedia.org/w/index.php?title=Bandas_y_grupos_emergentes_en_Colombia&oldid=172364243
    },
    # # Black Cobra: https://en.wikipedia.org/w/index.php?title=Black_Cobra_%28gang%29&oldid=1339076836
    # "Black Cobra": {
    #     "Black Cobra MC",  # (high) Record A is a street gang in Denmark, while Record B is an outlaw motorcycle club, likely in a different country, with no evidence of being the same group.
    #     #     ↳ Black Cobra MC: https://en.wikipedia.org/w/index.php?title=Bandidos_MC_criminal_allegations_and_incidents&oldid=1339076715
    #     "Black Cobra Motorcycle Club",  # (high) Record A is a street gang in Denmark, while Record B is a motorcycle club, likely in a different country, with no connection.
    #     #     ↳ Black Cobra Motorcycle Club: https://en.wikipedia.org/w/index.php?title=Bandidos_MC_criminal_allegations_and_incidents&oldid=1339076715
    # },
    # Bonde do Maluco: https://en.wikipedia.org/w/index.php?title=Bonde_do_Maluco&oldid=1353852850 | https://pt.wikipedia.org/w/index.php?title=Bonde_do_Maluco&oldid=71938521
    "Bonde do Maluco": {
        "Bonnie and Clyde",  # (high) Record A is a Brazilian criminal organization called Bonde do Maluco, while Record B refers to the historical American criminal duo Bonnie and Clyde, which is a completely different entity.
        #     ↳ Bonnie and Clyde: https://pt.wikipedia.org/w/index.php?title=Terceiro_Comando_Puro&oldid=71856792
    },
    # Brödraskapet: https://en.wikipedia.org/w/index.php?title=Br%C3%B6draskapet&oldid=1339076999
    "Brödraskapet": {
        "The Brotherhood",  # (high) Record A is a Swedish prison gang, while Record B is a UK motorcycle club chapter; different countries, types, and contexts.
        #     ↳ The Brotherhood: https://en.wikipedia.org/w/index.php?title=Black_Pistons_Motorcycle_Club&oldid=1351074542
    },
    # Cartel del Noreste: https://es.wikipedia.org/w/index.php?title=C%C3%A1rtel_del_Noreste&oldid=173553214
    "Cartel del Noreste": {
        "Cartel do Norte",  # (high) Cartel del Noreste is a Mexican Zetas splinter, while Cartel do Norte is a Brazilian Família do Norte splinter, with different countries and origins.
        #     ↳ Cartel do Norte: https://pt.wikipedia.org/w/index.php?title=Fam%C3%ADlia_do_Norte&oldid=71585855
    },
    # # Cataldo 'ndrina: https://en.wikipedia.org/w/index.php?title=Cataldo_%27ndrina&oldid=1339077145
    # "Cataldo 'ndrina": {
    #     "Cataldo clan",  # (low) Record B describes a clan allied with the De Stefanos, which is not mentioned in Record A, and the time period differs.
    #     #     ↳ Cataldo clan: https://en.wikipedia.org/w/index.php?title=First_%27Ndrangheta_war&oldid=1315743290
    # },
    # Clan dei Casamonica: https://it.wikipedia.org/w/index.php?title=Clan_dei_Casamonica&oldid=150463758
    "Clan dei Casamonica": {
        "Clan degli Spada",  # (high) Record B describes a separate allied family, not the Casamonica clan itself.
        #     ↳ Clan degli Spada: https://it.wikipedia.org/w/index.php?title=Clan_dei_Casamonica&oldid=150463758
    },
    # Clan Russo dei Quartieri Spagnoli: https://it.wikipedia.org/w/index.php?title=Clan_Russo_dei_Quartieri_Spagnoli&oldid=149861225
    "Clan Russo dei Quartieri Spagnoli": {
        "Clan Russo",  # (high) Clan Russo dei Quartieri Spagnoli is based in Naples, while Clan Russo from Nola is a different Camorra clan with distinct alliances.
        #     ↳ Clan Russo: https://it.wikipedia.org/w/index.php?title=Clan_Cava&oldid=150796621
        "Clan Russo (Nola)",  # (high) Different locations (Naples vs Nola) and different leadership (sons of Domenico Russo vs Pasquale Russo).
        #     ↳ Clan Russo (Nola): https://it.wikipedia.org/w/index.php?title=Clan_Alfieri&oldid=150454082
    },
    # Clan Russo di Nola: https://it.wikipedia.org/w/index.php?title=Clan_Russo_di_Nola&oldid=148550475
    "Clan Russo di Nola": {
        "Russo",  # (high) Record A is the Clan Russo di Nola operating in Nola, while Record B is a Russo clan from Quartieri Spagnoli in Naples, a different location and likely a different group.
        #     ↳ Russo: https://it.wikipedia.org/w/index.php?title=Camorra&oldid=150599702
    },
    # # Coffin Cheaters: https://en.wikipedia.org/w/index.php?title=Coffin_Cheaters&oldid=1351116258
    # "Coffin Cheaters": {
    #     "Coffin Cheaters MC (US)",  # (high) Record A describes an Australian club founded in 1970, while Record B is a US club founded in the 1960s, and the description explicitly states they are unrelated.
    #     #     ↳ Coffin Cheaters MC (US): https://en.wikipedia.org/w/index.php?title=Coffin_Cheaters&oldid=1351116258
    # },
    # Commisso 'ndrina: https://en.wikipedia.org/w/index.php?title=Commisso_%27ndrina&oldid=1351311101
    # "Commisso 'ndrina": {
    #     "Antonio Macrì",  # (high) Record A is a clan named after the Commisso family, while Record B is an individual boss named Antonio Macrì, who founded the clan but is not the same entity.
    #     #     ↳ Antonio Macrì: https://en.wikipedia.org/w/index.php?title=Piromalli_%27ndrina&oldid=1339086202
    #     "Macrì",  # (high) Record A is the Commisso 'ndrina, while Record B refers to the Macrì clan, a distinct group from an earlier period.
    #     #     ↳ Macrì: https://it.wikipedia.org/w/index.php?title=%27Ndrangheta&oldid=150697108
    # },
    # # Devil's Disciples Motorcycle Club (Canada): https://en.wikipedia.org/w/index.php?title=Devil%27s_Disciples_Motorcycle_Club_%28Canada%29&oldid=1341248245
    # "Devil's Disciples Motorcycle Club (Canada)": {
    #     "Devil's Disciples",  # (high) Record A is a Canadian outlaw motorcycle club founded in 1965, while Record B is a Chicago street gang from 1958 that evolved into Black Disciples.
    #     #     ↳ Devil's Disciples: https://en.wikipedia.org/w/index.php?title=Black_Disciples&oldid=1353106894
    #     "Devils Disciples",  # (high) Record A describes a Canadian MC that disbanded in 1976, while Record B refers to former members in the 1990s, likely a different group.
    #     #     ↳ Devils Disciples: https://en.wikipedia.org/w/index.php?title=Quebec_Biker_War&oldid=1339848893
    # },
    # Diablos Motorcycle Club: https://en.wikipedia.org/w/index.php?title=Diablos_Motorcycle_Club&oldid=1354073110 | https://en.wikipedia.org/w/index.php?title=Diablos_Motorcycle_Club_%28founded_1999%29&oldid=1350095302
    # "Diablos Motorcycle Club": {
    #     "Diablos",  # (high) Record A is a US-based club founded in California, while Record B is an Australian club; different countries and no evidence they are the same entity.
    #     #     ↳ Diablos: https://en.wikipedia.org/w/index.php?title=Hells_Angels_MC_criminal_allegations_and_incidents_in_Australia&oldid=1346691638
    #     "Diablos MC",  # (medium) Record A is a US-based club founded in California, while Record B is an Australian/European club allied with Bandidos, likely a different entity.
    #     #     ↳ Diablos MC: https://en.wikipedia.org/w/index.php?title=Bandidos_MC_criminal_allegations_and_incidents&oldid=1339076715
    # },
    # # Dirty Dozen Motorcycle Club: https://en.wikipedia.org/w/index.php?title=Dirty_Dozen_Motorcycle_Club&oldid=1345368766
    # "Dirty Dozen Motorcycle Club": {
    #     "Dirty Dozen",  # (low) Record A is a specific outlaw motorcycle club in Arizona, while Record B is a generic rival gang with no location or time period, making it uncertain if they are the same.
    #     #     ↳ Dirty Dozen: https://en.wikipedia.org/w/index.php?title=Savage_Nomads&oldid=1298518382
    # },
    # FARC dissidents: https://en.wikipedia.org/w/index.php?title=FARC_dissidents&oldid=1354228099
    "FARC dissidents": {
        "FARC-EP",  # (high) Record A refers to post-2016 dissident factions, while Record B is the original FARC-EP that existed from 1964 to 2017.
        #     ↳ FARC-EP: https://en.wikipedia.org/w/index.php?title=Jalisco_New_Generation_Cartel&oldid=1354184178
    },
    # Forty Thieves: https://en.wikipedia.org/w/index.php?title=Forty_Thieves_%28New_York_gang%29&oldid=1339077715
    "Forty Thieves": {
        "Forty Thieves (London gang)",  # (high) Different gangs in different cities (New York vs London) with no connection.
        #     ↳ Forty Thieves (London gang): https://en.wikipedia.org/w/index.php?title=Forty_Thieves_%28New_York_gang%29&oldid=1339077715
        "Forty Thieves (Philadelphia gang)",  # (medium) Different cities (New York vs. Philadelphia) and no evidence linking them as the same group.
        #     ↳ Forty Thieves (Philadelphia gang): https://en.wikipedia.org/w/index.php?title=Forty_Thieves_%28New_York_gang%29&oldid=1339077715
    },
    # Gangster Disciples: https://en.wikipedia.org/w/index.php?title=Gangster_Disciples&oldid=1353938599
    "Gangster Disciples": {
        "6th Rotterdam Gangster Disciples",  # (high) Record A is the original US-based Gangster Disciples, while Record B is a Dutch set that is a separate local chapter, not the same overarching organization.
        #     ↳ 6th Rotterdam Gangster Disciples: https://en.wikipedia.org/w/index.php?title=List_of_gangs_in_the_Netherlands&oldid=1355175034
        "Amsterdam Insane Gangster Disciples",  # (high) Record A is a US-based gang founded in Chicago, while Record B is a Dutch street gang that is a local set, not the same overarching organization.
        #     ↳ Amsterdam Insane Gangster Disciples: https://en.wikipedia.org/w/index.php?title=List_of_gangs_in_the_Netherlands&oldid=1355175034
    },
    # Genovese crime family: https://en.wikipedia.org/w/index.php?title=Genovese_crime_family&oldid=1354193966
    "Genovese crime family": {
        "Westside Mob",  # (high) Record A is the modern Genovese crime family, while Record B is a 1920s faction with a different name and leadership.
        #     ↳ Westside Mob: https://en.wikipedia.org/w/index.php?title=Detroit_Partnership&oldid=1353846660
    },
    # Gitans Moto Club: https://en.wikipedia.org/w/index.php?title=Gitans_Moto_Club&oldid=1322245000
    "Gitans Moto Club": {
        "Hells Angels Vikings",  # (high) Record A is a French-Canadian gang that became Hells Angels in 1984, while Record B is an unsanctioned gang in Surrey, UK, with no connection.
        #     ↳ Hells Angels Vikings: https://en.wikipedia.org/w/index.php?title=Hells_Angels_MC_criminal_allegations_and_incidents_in_the_United_Kingdom&oldid=1323829541
        "Vikings Motorcycle Club",  # (high) Record A's Vikings was a 1960s street gang in Quebec that evolved into Gitans MC, while Record B's Vikings MC is a separate club in Australia feuding with Bandidos in 1995.
        #     ↳ Vikings Motorcycle Club: https://en.wikipedia.org/w/index.php?title=Bandidos_MC_criminal_allegations_and_incidents_in_Australia&oldid=1348186117
        "Vikings MC",  # (high) Record A's Vikings was a 1960s street gang that evolved into Gitans MC, while Record B's Vikings MC is a separate club that existed from 1975 onward, likely a different entity.
        #     ↳ Vikings MC: https://en.wikipedia.org/w/index.php?title=Hells_Angels_MC_criminal_allegations_and_incidents_in_the_United_Kingdom&oldid=1323829541
    },
    # Greco Mafia clan: https://en.wikipedia.org/w/index.php?title=Greco_Mafia_clan&oldid=1339077910
    "Greco Mafia clan": {
        "'Ndrina Greco",  # (high) Record A is a Sicilian Mafia clan, while Record B is a 'Ndrina (Calabrian Mafia group) with a different leader and alliance.
        #     ↳ 'Ndrina Greco: https://it.wikipedia.org/w/index.php?title=%27Ndrina_Grande_Aracri&oldid=149624208
    },
    # Green Gang: https://en.wikipedia.org/w/index.php?title=Green_Gang&oldid=1352003460
    "Green Gang": {
        "Green Street boys",  # (high) Record A is a historical Chinese triad, while Record B is a modern Dutch street gang with no connection.
        #     ↳ Green Street boys: https://en.wikipedia.org/w/index.php?title=List_of_gangs_in_the_Netherlands&oldid=1355175034
    },
    # Green Street Counts: https://en.wikipedia.org/w/index.php?title=Green_Street_Counts&oldid=1323757336
    "Green Street Counts": {
        "Green Street Counts (West)",  # (high) Record A explicitly states there is a separate unaffiliated group with the same name in West Philadelphia, which matches Record B's description.
        #     ↳ Green Street Counts (West): https://en.wikipedia.org/w/index.php?title=Green_Street_Counts&oldid=1323757336
    },
    # # Hermanos de Pistoleros Latinos: https://en.wikipedia.org/w/index.php?title=Hermanos_de_Pistoleros_Latinos&oldid=1326819040
    # "Hermanos de Pistoleros Latinos": {
    #     "HPL 45s",  # (high) Record B is a splinter faction of HPL, not the same organization as the main HPL gang.
    #     #     ↳ HPL 45s: https://en.wikipedia.org/w/index.php?title=Hermanos_de_Pistoleros_Latinos&oldid=1326819040
    # },
    # Hezbollah: https://en.wikipedia.org/w/index.php?title=Hezbollah&oldid=1354277658
    "Hezbollah": {
        "Kurdish Hezbollah",  # (high) Hezbollah is a Lebanese Shia group, while Kurdish Hezbollah is a separate Kurdish group opposed to the PKK.
        #     ↳ Kurdish Hezbollah: https://en.wikipedia.org/w/index.php?title=Kurdistan_Workers%27_Party&oldid=1349828925
    },
    # Highwaymen Motorcycle Club: https://en.wikipedia.org/w/index.php?title=Highwaymen_Motorcycle_Club&oldid=1351127102
    "Highwaymen Motorcycle Club": {
        "Highwaymen",  # (high) Record A is a large US-based outlaw MC founded in 1954, while Record B is a small Canadian club that became a Hells Angels puppet in 1983.
        #     ↳ Highwaymen: https://en.wikipedia.org/w/index.php?title=Hells_Angels_MC_criminal_allegations_and_incidents_in_British_Columbia&oldid=1327107209
    },
    # Houthis: https://en.wikipedia.org/w/index.php?title=Houthis&oldid=1353466278
    "Houthis": {
        "Ansar Allah",  # (high) Record A describes the Yemeni Houthi movement (Ansar Allah), while Record B describes a Palestinian group associated with Hezbollah, which are distinct entities.
        #     ↳ Ansar Allah: https://en.wikipedia.org/w/index.php?title=Hezbollah&oldid=1354277658
    },
    # Irish mob: https://en.wikipedia.org/w/index.php?title=Irish_mob&oldid=1351558423
    "Irish mob": {
        "Oklahoma Irish Mob",  # (high) Record A describes a broad ethnic organized crime network in the US and Canada, while Record B is a specific prison gang in Oklahoma, distinct in scope and location.
        #     ↳ Oklahoma Irish Mob: https://en.wikipedia.org/w/index.php?title=Irish_mob&oldid=1351558423
    },
    # Islamic State: https://en.wikipedia.org/w/index.php?title=Islamic_State&oldid=1354566840
    "Islamic State": {
        "Islamic State in Somalia",  # (high) Record A is the main Islamic State group, while Record B is a regional affiliate in Somalia, which is a distinct entity.
        #     ↳ Islamic State in Somalia: https://en.wikipedia.org/w/index.php?title=Al-Shabaab_%28militant_group%29&oldid=1353959560
    },
    # Islamic State – Somalia Province: https://en.wikipedia.org/w/index.php?title=Islamic_State_%E2%80%93_Somalia_Province&oldid=1352751388
    "Islamic State – Somalia Province": {
        "Islamic State – Sahel Province",  # (high) One operates in Somalia, the other in the Sahel region of West Africa; they are distinct geographic branches of IS.
        #     ↳ Islamic State – Sahel Province: https://en.wikipedia.org/w/index.php?title=Islamic_State&oldid=1354566840
    },
    # Juárez Cartel: https://en.wikipedia.org/w/index.php?title=Ju%C3%A1rez_Cartel&oldid=1350705540 | https://es.wikipedia.org/w/index.php?title=C%C3%A1rtel_de_Ju%C3%A1rez&oldid=172724083
    "Juárez Cartel": {
        "Nuevo Cartel de Juárez",  # (high) Record B is a splinter group of the Juárez Cartel, not the same organization.
        #     ↳ Nuevo Cartel de Juárez: https://en.wikipedia.org/w/index.php?title=Ju%C3%A1rez_Cartel&oldid=1350705540
        "Nuevo Cártel de Juárez",  # (high) Record B is a splinter group that broke away from the original Juárez Cartel, making them distinct entities.
        #     ↳ Nuevo Cártel de Juárez: https://es.wikipedia.org/w/index.php?title=La_L%C3%ADnea&oldid=172511186
    },
    # La Empresa: https://en.wikipedia.org/w/index.php?title=La_Empresa&oldid=1332164490
    "La Empresa": {
        "The Enterprise",  # (high) Record A is a Colombian drug cartel, while Record B is a secret U.S. organization from the 1980s supporting Contras.
        #     ↳ The Enterprise: https://en.wikipedia.org/w/index.php?title=Contras&oldid=1351978298
    },
    # La Familia Michoacana: https://en.wikipedia.org/w/index.php?title=La_Familia_Michoacana&oldid=1352254381 | https://es.wikipedia.org/w/index.php?title=La_Familia_Michoacana&oldid=172770526
    "La Familia Michoacana": {
        "La Familia",  # (high) Record A is a Mexican drug cartel, while Record B is a motorcycle gang involving the Al-Zein family, which are distinct entities.
        #     ↳ La Familia: https://en.wikipedia.org/w/index.php?title=Al-Zein_crime_family&oldid=1346406301
    },
    # # Los Cachiros: https://en.wikipedia.org/w/index.php?title=Cachiros&oldid=1341692169 | https://es.wikipedia.org/w/index.php?title=Los_Cachiros&oldid=172023132
    # "Los Cachiros": {
    #     "Cachiros",  # (low) Record A describes a Honduran drug cartel, while Record B mentions an ally of Armenian Power, likely a different group in a different region.
    #     #     ↳ Cachiros: https://en.wikipedia.org/w/index.php?title=Armenian_Power&oldid=1340540308
    # },
    # Los Lobos: https://en.wikipedia.org/w/index.php?title=Los_Lobos_%28gang%29&oldid=1344237609 | https://es.wikipedia.org/w/index.php?title=Los_Lobos_%28pandilla%29&oldid=173291283 | https://pt.wikipedia.org/w/index.php?title=Los_Lobos_%28quadrilha%29&oldid=70002102
    "Los Lobos": {
        "Los Lobos Motorcycle Club",  # (high) Record A is an Ecuadorian criminal organization formed in 2020, while Record B is an Ontario motorcycle club that disbanded in 2000.
        #     ↳ Los Lobos Motorcycle Club: https://en.wikipedia.org/w/index.php?title=Quebec_Biker_War&oldid=1339848893
    },
    # Los Viagras: https://en.wikipedia.org/w/index.php?title=Los_Viagras&oldid=1343759245 | https://es.wikipedia.org/w/index.php?title=Los_Viagras&oldid=172526054
    "Los Viagras": {
        "Los Sierra",  # (high) Los Viagras is an armed wing of LNFM Cartel in Michoacán, while Los Sierra is a separate group created by the Gulf Cartel.
        #     ↳ Los Sierra: https://es.wikipedia.org/w/index.php?title=C%C3%A1rtel_del_Golfo&oldid=173539252
    },
    # Los Zetas: https://en.wikipedia.org/w/index.php?title=Los_Zetas&oldid=1353699517 | https://es.wikipedia.org/w/index.php?title=Los_Zetas&oldid=173510566
    "Los Zetas": {
        "Talibans",  # (high) Los Zetas is a Mexican cartel, while Talibans is a Haitian gang; they are different organizations despite the alias overlap.
        #     ↳ Talibans: https://en.wikipedia.org/w/index.php?title=Timeline_of_gang-related_events_in_Haiti&oldid=1348653135
        # Splinter cartels wrongly carried as Los Zetas aliases (manual review 2026-06-14):
        "Zetas Vieja Escuela",  # (high) ZVE is a 2014 splinter cartel from Los Zetas (Veracruz); a distinct org.
        #     ↳ Zetas Vieja Escuela: https://en.wikipedia.org/w/index.php?title=Zetas_Vieja_Escuela&oldid=1356647809
        "Los Talibanes",  # (high) 2012 Zetas splinter led by Z-50 (distinct from the Haitian 'Talibans' above); a separate cartel.
        #     ↳ Los Talibanes: https://en.wikipedia.org/w/index.php?title=Los_Talibanes&oldid=1344998496
        "Cartel del Noreste",  # (high) CDN is a Zetas successor/splinter cartel, a distinct org.
        #     ↳ Cártel del Noreste: https://en.wikipedia.org/w/index.php?title=C%C3%A1rtel_del_Noreste&oldid=1358799590
    },
    # Luppino crime family: https://en.wikipedia.org/w/index.php?title=Luppino_crime_family&oldid=1322469039
    "Luppino crime family": {
        "'Ndrina Luppino",  # (low) Record A is a Canadian 'Ndrangheta family, while Record B is a generic 'Ndrina related to Mammoliti, likely in Italy, with no clear connection.
        #     ↳ 'Ndrina Luppino: https://it.wikipedia.org/w/index.php?title=%27Ndrina_Mammoliti&oldid=149624603
    },
    # Maniac Latin Disciples: https://en.wikipedia.org/w/index.php?title=Maniac_Latin_Disciples&oldid=1347047192
    "Maniac Latin Disciples": {
        "Latin Scorpions",  # (high) The Maniac Latin Disciples and Latin Scorpions are distinct gangs with different names and no evidence of being the same organization.
        #     ↳ Latin Scorpions: https://en.wikipedia.org/w/index.php?title=Spanish_Gangster_Disciples&oldid=1343765771
    },
    # Mongols Motorcycle Club: https://en.wikipedia.org/w/index.php?title=Mongols_MC_criminal_allegations_and_incidents&oldid=1336330137 | https://en.wikipedia.org/w/index.php?title=Mongols_Motorcycle_Club&oldid=1351565189
    "Mongols Motorcycle Club": {
        "Mongols MC (Quebec)",  # (high) Record B describes a Quebec-based club that was absorbed by Popeyes in 1974, unrelated to the international Mongols MC founded in 1969.
        #     ↳ Mongols MC (Quebec): https://en.wikipedia.org/w/index.php?title=Popeye_Moto_Club&oldid=1339935529
    },
    # New Irish Republican Army: https://en.wikipedia.org/w/index.php?title=New_Irish_Republican_Army&oldid=1352677234
    "New Irish Republican Army": {
        "Irish Republican Army",  # (high) Record A describes the New IRA formed in 2012, while Record B describes the original IRA from 1919-1922, which are distinct organizations.
        #     ↳ Irish Republican Army: https://en.wikipedia.org/w/index.php?title=Death_squad&oldid=1354269824
    },
    # New Zealand Nomads: https://en.wikipedia.org/w/index.php?title=New_Zealand_Nomads&oldid=1353856653
    "New Zealand Nomads": {
        "Nomads",  # (high) Record A is a New Zealand criminal gang, while Record B is a Scottish motorcycle club from a different era and location.
        #     ↳ Nomads: https://en.wikipedia.org/w/index.php?title=Blue_Angels_Motorcycle_Club&oldid=1353407306
        "Nomads Motorcycle Club",  # (high) Record A describes a New Zealand criminal gang, while Record B describes a motorcycle club with different affiliations and location.
        #     ↳ Nomads Motorcycle Club: https://en.wikipedia.org/w/index.php?title=Bandidos_MC_criminal_allegations_and_incidents_in_Australia&oldid=1348186117
    },
    # North Side Gang: https://en.wikipedia.org/w/index.php?title=North_Side_Gang&oldid=1346761976
    "North Side Gang": {
        "North Side",  # (high) Record A is a Prohibition-era Chicago gang, while Record B is a Dutch football hooligan group.
        #     ↳ North Side: https://en.wikipedia.org/w/index.php?title=F-side&oldid=1348233369
    },
    # Oficina de Envigado: https://en.wikipedia.org/w/index.php?title=Oficina_de_Envigado&oldid=1339085917 | https://es.wikipedia.org/w/index.php?title=Oficina_de_Envigado&oldid=171967973
    "Oficina de Envigado": {
        "La Oficina",  # (high) Record A is a Colombian cartel, while Record B is a splinter group in Mexico, indicating different organizations.
        #     ↳ La Oficina: https://en.wikipedia.org/w/index.php?title=Beltr%C3%A1n-Leyva_Organization&oldid=1352425846
    },
    # Organised crime in India: https://en.wikipedia.org/w/index.php?title=Organised_crime_in_India&oldid=1339085963
    "Organised crime in India": {
        "Indian Mafia",  # (high) Record A describes organized crime in India broadly, while Record B is a specific street gang in Canada with a different scope and location.
        #     ↳ Indian Mafia: https://en.wikipedia.org/w/index.php?title=Indigenous-based_organized_crime&oldid=1318019845
    },
    # Organised crime in Nigeria: https://en.wikipedia.org/w/index.php?title=Organised_crime_in_Nigeria&oldid=1339973705
    "Organised crime in Nigeria": {
        "Nigerian gangs",  # (high) Record A describes organized crime in Nigeria broadly, while Record B specifically refers to Nigerian gangs operating in Italy, which are distinct groups.
        #     ↳ Nigerian gangs: https://en.wikipedia.org/w/index.php?title=Camorra&oldid=1351807158
    },
    # Original Red Devils Motorcycle Club: https://en.wikipedia.org/w/index.php?title=Original_Red_Devils_Motorcycle_Club&oldid=1339086020
    "Original Red Devils Motorcycle Club": {
        "Red Devils",  # (high) Different countries (Canada vs Australia) and distinct histories; Canadian club patched over in 2014, Australian club is a separate entity aligned with Hells Angels.
        #     ↳ Red Devils: https://en.wikipedia.org/w/index.php?title=Hells_Angels_MC_criminal_allegations_and_incidents_in_Australia&oldid=1346691638
    },
    # Peckham Boys: https://en.wikipedia.org/w/index.php?title=Peckham_Boys&oldid=1354494846
    "Peckham Boys": {
        "Chestnut Estate",  # (high) Record A describes the Peckham Boys gang in South London, while Record B describes a splinter group from Tottenham Mandem, a different gang in North London.
        #     ↳ Chestnut Estate: https://en.wikipedia.org/w/index.php?title=Tottenham_Mandem&oldid=1343929340
    },
    # Philadelphia crime family: https://en.wikipedia.org/w/index.php?title=Philadelphia_crime_family&oldid=1347707905
    "Philadelphia crime family": {
        "'Ndrina Bruno",  # (high) Record A is an Italian-American Mafia family in Philadelphia, while Record B is a 'Ndrina (Calabrian Mafia group) in Italy, with no connection.
        #     ↳ 'Ndrina Bruno: https://it.wikipedia.org/w/index.php?title=%27Ndrina_Vallelunga&oldid=149663894
    },
    # Popular Liberation Army: https://en.wikipedia.org/w/index.php?title=Popular_Liberation_Army&oldid=1344078114
    "Popular Liberation Army": {
        "People's Liberation Army",  # (high) Record A is a Colombian guerrilla group (EPL), while Record B is the armed wing of the Mantaro faction, likely in Peru, with no connection.
        #     ↳ People's Liberation Army: https://en.wikipedia.org/w/index.php?title=Shining_Path&oldid=1353659817
    },
    # Primeiro Comando da Capital: https://en.wikipedia.org/w/index.php?title=Primeiro_Comando_da_Capital&oldid=1353385453 | https://pt.wikipedia.org/w/index.php?title=Primeiro_Comando_da_Capital&oldid=72237909
    "Primeiro Comando da Capital": {
        "Colombian Communist Party",  # (high) Record A is a Brazilian criminal syndicate (PCC), while Record B is a Colombian political party with no relation.
        #     ↳ Colombian Communist Party: https://en.wikipedia.org/w/index.php?title=Revolutionary_Armed_Forces_of_Colombia&oldid=1354228238
    },
    # Provisional Irish Republican Army: https://en.wikipedia.org/w/index.php?title=Provisional_Irish_Republican_Army&oldid=1353790505
    "Provisional Irish Republican Army": {
        "Irish Republican Army (1919-1922)",  # (high) Record A is the Provisional IRA from 1969 onward, while Record B is the original IRA from 1919-1922, distinct entities.
        #     ↳ Irish Republican Army (1919-1922): https://en.wikipedia.org/w/index.php?title=Real_Irish_Republican_Army&oldid=1347283468
        "Irish Republican Army",  # (high) Record A refers to the Provisional IRA formed in 1969, while Record B refers to the original IRA active 1919-1922, which are distinct organizations.
        #     ↳ Irish Republican Army: https://en.wikipedia.org/w/index.php?title=Death_squad&oldid=1354269824
    },
    # Puccio family: https://en.wikipedia.org/w/index.php?title=Puccio_family&oldid=1351523384
    "Puccio family": {
        "'Ndrina Puccio",  # (high) Record A is an Argentine criminal family, while Record B is an Italian 'Ndrangheta clan; they are unrelated despite similar names.
        #     ↳ 'Ndrina Puccio: https://it.wikipedia.org/w/index.php?title=%27Ndrina_Maesano&oldid=149624562
    },
    # Real Calton Tongs: https://en.wikipedia.org/w/index.php?title=Tongland_%28gang_area%29&oldid=1280147052
    "Real Calton Tongs": {
        "The Tongs",  # (high) Record A is a 1960s teenage gang, while Record B is an early 20th century gang; different time periods and descriptions.
        #     ↳ The Tongs: https://en.wikipedia.org/w/index.php?title=Penny_Mobs&oldid=1320090313
        "Tongs",  # (high) Record A is a 1960s Scottish teenage gang, while Record B is a Chinese organized crime group from the late 1800s-early 1900s.
        #     ↳ Tongs: https://en.wikipedia.org/w/index.php?title=Gangs_in_the_United_States&oldid=1352742881
    },
    # Rebels Motorcycle Club: https://en.wikipedia.org/w/index.php?title=Rebels_Motorcycle_Club&oldid=1351250697
    "Rebels Motorcycle Club": {
        "Confederates Motorcycle Club",  # (high) The Rebels MC (Australia) and Confederates MC (Netherlands) are distinct clubs; 'Confederates' is an alias of Rebels MC but the description indicates a separate Dutch club.
        #     ↳ Confederates Motorcycle Club: https://en.wikipedia.org/w/index.php?title=List_of_gangs_in_the_Netherlands&oldid=1355175034
    },
    # Rockers Motor Club: https://en.wikipedia.org/w/index.php?title=Rockers_Motor_Club&oldid=1347361915
    "Rockers Motor Club": {
        "Montreal Rockers Motorcycle Club",  # (high) Record A is a Hells Angels support club founded in 1992, while Record B is a different gang from the 1970s that became an Outlaws chapter.
        #     ↳ Montreal Rockers Motorcycle Club: https://en.wikipedia.org/w/index.php?title=Rockers_Motor_Club&oldid=1347361915
    },
    # Rudaj Organization: https://en.wikipedia.org/w/index.php?title=Rudaj_Organization&oldid=1336995164
    "Rudaj Organization": {
        "The Corporation",  # (high) Record A is an Albanian-American group in NYC, while Record B is an African-American group in Harlem; they are distinct organizations despite the shared alias.
        #     ↳ The Corporation: https://en.wikipedia.org/w/index.php?title=East_Harlem_Purple_Gang&oldid=1336602552
    },
    # Spanish Gangster Disciples: https://en.wikipedia.org/w/index.php?title=Spanish_Gangster_Disciples&oldid=1343765771
    "Spanish Gangster Disciples": {
        "Spanish Growth and Development",  # (high) Record A is a street gang in Chicago, while Record B is a short-lived alliance of different gangs for a specific purpose.
        #     ↳ Spanish Growth and Development: https://en.wikipedia.org/w/index.php?title=Spanish_Cobras&oldid=1352690694
    },
    # Taliban: https://en.wikipedia.org/w/index.php?title=Taliban&oldid=1354808985
    "Taliban": {
        "Taliban (Kenyan gang)",  # (high) Record A is an Afghan political/militant group, while Record B is a Kenyan criminal gang with no connection.
        #     ↳ Taliban (Kenyan gang): https://en.wikipedia.org/w/index.php?title=Taliban_%28gang%29&oldid=1338833033
    },
    # # Tambovskaya Bratva: https://en.wikipedia.org/w/index.php?title=Tambovskaya_Bratva&oldid=1339086896 | https://en.wikipedia.org/w/index.php?title=Vladimir_Kumarin&oldid=1308049550
    # "Tambovskaya Bratva": {
    #     "Tambov gang",  # (medium) Record A is a large Russian mafia based in Saint Petersburg, while Record B is a Russian-speaking crime group in Düsseldorf with different activities, likely a separate entity.
    #     #     ↳ Tambov gang: https://en.wikipedia.org/w/index.php?title=Crime_in_Germany&oldid=1353030176
    # },
    # Texas Syndicate: https://en.wikipedia.org/w/index.php?title=Texas_Syndicate&oldid=1328954742 | https://es.wikipedia.org/w/index.php?title=Sindicato_de_Texas&oldid=173536157
    "Texas Syndicate": {
        "Texas Mafia",  # (high) Texas Syndicate and Texas Mafia are distinct prison gangs with different names and histories.
        #     ↳ Texas Mafia: https://en.wikipedia.org/w/index.php?title=Aryan_Brotherhood_of_Texas&oldid=1344693396
    },
    # The Commission: https://en.wikipedia.org/w/index.php?title=The_Commission_%28American_Mafia%29&oldid=1349386711
    "The Commission": {
        "Sicilian Mafia Commission",  # (high) The American Mafia Commission and the Sicilian Mafia Commission are distinct governing bodies in different countries.
        #     ↳ Sicilian Mafia Commission: https://en.wikipedia.org/w/index.php?title=Angelo_La_Barbera&oldid=1341535754
    },
    # The Firm: https://en.wikipedia.org/w/index.php?title=Kray_twins&oldid=1354206018
    "The Firm": {
        "The British",  # (high) Record A is a specific 1960s London gang, while Record B is a corporate body for gangs aligned against The Americans, likely from a different context.
        #     ↳ The British: https://en.wikipedia.org/w/index.php?title=The_Americans_%28gang%29&oldid=1339506060
    },
    # Tottenham Mandem: https://en.wikipedia.org/w/index.php?title=Tottenham_Mandem&oldid=1343929340
    "Tottenham Mandem": {
        "Tottenham Boys",  # (high) Tottenham Mandem is a street gang based in Tottenham, while Tottenham Boys is a Kurdish gang funneling profits to the PKK, with different origins and activities.
        #     ↳ Tottenham Boys: https://en.wikipedia.org/w/index.php?title=Kurdish_mafia&oldid=1351997479
    },
    # Tribesmen Motorcycle Club: https://en.wikipedia.org/w/index.php?title=Tribesmen_Motorcycle_Club&oldid=1351413837
    "Tribesmen Motorcycle Club": {
        "Tribesmen",  # (high) Record A is a New Zealand outlaw motorcycle club, while Record B is a Canadian biker gang that became Hells Angels in 1983.
        #     ↳ Tribesmen: https://en.wikipedia.org/w/index.php?title=Hells_Angels_MC_criminal_allegations_and_incidents_in_British_Columbia&oldid=1327107209
    },
    "Đại Cathay": {
        "Four Great Kings",  # (low) No evidence links 'Đại Cathay' to the 'Four Great Kings' group; names and descriptions differ.
        #     ↳ Four Great Kings: https://en.wikipedia.org/w/index.php?title=History_of_organized_crime_in_Saigon&oldid=1256307087
    },
}

# ═══════════════════════════════════════════════════════════════════
# TO_BE_EXCLUDED
# Hand-curated exclusions
# Lowercase names of nodes that should be removed entirely.
# These are typically non-criminal entities (governments, NGOs,
# political parties) that the LLM mistakenly extracted as crime groups.
# ═══════════════════════════════════════════════════════════════════
TO_BE_EXCLUDED = {
    # Cybercrime is out of scope for now (not following cyber groups yet). LockBit
    # is a ransomware-as-a-service crew, not a traditional organized-crime group;
    # drop it. See the cybercrime scope note in the README. (Keep this a set, not
    # a dict: TO_BE_EXCLUDED = {} would be an empty dict.)
    "LockBit",
}

# ═══════════════════════════════════════════════════════════════════
# EDGE_BLOCKLIST
# Drop org→org relationship EDGES whose evidence doesn't support them —
# two orgs that don't really have the stated relationship, or an edge
# typed wrong (found by audit/2_review_edges.py).
#
# Format: { org: { other_org: {relationship, ...} } }
#   - the inner set lists the relationship types to drop between the two
#     orgs — any of "cooperation", "conflict", "other";
#   - use "*" to drop EVERY edge between them, of any type.
# SYMMETRIC: write each pair once; it is applied in BOTH directions.
# Names fold()-match (case/accents-insensitive), so use each org's
# canonical standard_name from crimenet.json.
#
# To populate: run audit/2_review_edges.py, review audit_data/2_edge_blocklist.py
# (each entry carries the model's reason + evidence quote + source URL),
# and paste the good entries here.
# ═══════════════════════════════════════════════════════════════════
EDGE_BLOCKLIST = {
    # "'Ndrangheta": {
    #     "Al-Qaeda": {"other"},   # only their structures were compared
    #     "Camorra": {"*"},        # drop every edge between them
    # },
}

# ═══════════════════════════════════════════════════════════════════
# COUNTRY_LINK_BLOCKLIST
# Drop org→country LINKS whose evidence doesn't support a real link — the
# country was only mentioned incidentally (found by audit/3_review_country_links.py).
# Removes the entry from a node's country_links (and the worldwide map).
#
# Format: { org: {country, ...} }. Country names match the strings in
# crimenet.json country_links, case-insensitively; org names fold()-match.
#
# To populate: run audit/3_review_country_links.py, review
# audit_data/3_country_link_blocklist.py, and paste the good entries here.
# ═══════════════════════════════════════════════════════════════════
COUNTRY_LINK_BLOCKLIST = {
    # "'Ndrangheta": {"Finland", "Slovakia"},
}