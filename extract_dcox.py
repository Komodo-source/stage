"""
Extrait un livret d'accueil (.docx ou données brutes) et l'envoie vers Airtable.

Usage:
    python airtable_import.py <input_file.docx> [--dry-run]

Tables Airtable ciblées:
    - Maison
    - Proprietaire
    - Livret
    - Piscine
    - DispositionMaison
    - equipement
    - WorkFlowBreezeway  (check-in / check-out tasks)
    - ConditionLocation  (conditions de location)
"""

import argparse
import os
import zipfile
import xml.etree.ElementTree as ET
import re
import json
import sys
from selenium import webdriver
import json
import time
from bs4 import BeautifulSoup
import requests

def strip(text):
    """Remove html tags from a string"""
    import re
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)


def clean_extracted_data(data: dict) -> dict:
    """Clean common formatting issues from Breezeway extraction"""

    # Clean house name: remove prefixes like "zAutre - M7 - "
    if data.get("nom_maison"):
        # Keep only the meaningful name after the last " - "
        parts = data["nom_maison"].split(" - ")
        data["nom_maison"] = parts[-1] if len(parts) > 1 else data["nom_maison"]

    # Clean address: remove trailing commas/spaces
    if data.get("adresse"):
        data["adresse"] = re.sub(r'[,;\s]+$', '', data["adresse"].strip())

    # Clean WiFi credentials (remove extra whitespace)
    if data.get("name_wifi"):
        data["name_wifi"] = data["name_wifi"].strip()
    if data.get("mdp_wifi"):
        data["mdp_wifi"] = data["mdp_wifi"].strip()

    if data.get("recommandation"):
            for reco in data["recommandation"].values():
                reco["rating"] = reco.get("rating") or 0
                reco["photo"] = reco.get("photo") or ""
                reco["summary"] = reco.get("summary") or ""
                reco["latitude"] = reco.get("latitude") or 0
                reco["longitude"] = reco.get("longitude") or 0
                reco["formatted_address"] = reco.get("formatted_address") or reco.get("city", "")


    return data

def extract_content(html):
    soup = BeautifulSoup(html, "html.parser")

    images = []
    videos = []
    img_counter = [0]
    vid_counter = [0]

    for img in soup.find_all("img"):
        src = img.get("src")
        if src:
            images.append(src)
            beacon = soup.new_string(f" $i{img_counter[0]} ")
            img.replace_with(beacon)
            img_counter[0] += 1

    for iframe in soup.find_all("iframe"):
        src = iframe.get("src")
        if src:
            videos.append(src)
            beacon = soup.new_string(f" $v{vid_counter[0]} ")
            iframe.replace_with(beacon)
            vid_counter[0] += 1

    text = soup.get_text(separator="\n").strip()

    return {
        "text": text,
        "images": images,
        "videos": videos
    }


def extract_data_breezeway():
    lst_url = []

    return_value = {}
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--enable-logging')
    chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

    driver = webdriver.Chrome(options=chrome_options)

    print("Navigating to the page...")
    driver.get("https://guide.breezeway.io/NpayeOvoM-Q/home/page/160614")

    # Wait for network requests to fire and resolve
    print("Waiting 10 seconds for requests to complete...")
    time.sleep(10)

    # Capture network log entries
    log_entries = driver.get_log("performance")
    print(f"Total performance logs captured: {len(log_entries)}\n")
    print("-" * 50)

    found_target = False

    for entry in log_entries:
        try:
            message_obj = json.loads(entry.get("message", "{}"))
            message = message_obj.get("message", {})
            method = message.get("method", "")

            if method == 'Network.responseReceived':
                params = message.get('params', {})
                response = params.get('response', {})

                response_url = response.get('url', '')
                response_code = response.get('status', '')
                #print(response_code)
                #print(response_url)
                if "https://api.breezeway.io/public/guides" in response_url:
                    print("✅ Value found json ")
                    print("retrieving values")
                    print(response_url)
                    lst_url.append(response_url)
                    data = requests.get(response_url).json()

                    return_value["nom_maison"] = (
                        data["home"]["name"]
                    )

                    return_value["adresse"] = (
                    data["home"]["address"]["address1"] + ", " +
                    data["home"]["address"]["city"] + ", " +
                    (data["home"]["address"].get("state") or "")
                )
                return_value["CICO"] = (
                    data["company"]["defaults"]["checkin_time"] + ";" +
                    data["company"]["defaults"]["checkout_time"]
                )
                return_value["photo_maison"] = (
                    "https://images.breezeway.io/" +
                    data["home"]["photo"]["bucket"] + "/" +
                    data["home"]["photo"]["photo_key"]
                )

                for page in data["pages"]:
                    title = page["title"]

                    if title == "Bienvenue":
                        for section in page["sections"]:
                            if section["title"] == "Accès":
                                return_value["html_bienvenue"] = strip(section["blocks"][0]["data"])
                            elif section["title"] == "Wifi":
                                return_value["name_wifi"] = section["blocks"][0]["data"]["wifi_name"]
                                return_value["mdp_wifi"] = section["blocks"][0]["data"]["wifi_password"]
                            elif section["title"] == "Règles de la maison":
                                return_value["rules"] = strip(section["blocks"][0]["data"]["content"])

                    elif title == "Points d'Attention !":
                        return_value["point_attention"] = {
                            section["title"]: extract_content(section["blocks"][0]["data"])
                            for section in page["sections"]
                        }

                    elif title == "Équipements intérieurs":
                        return_value["equippement_intérieur"] = {
                            section["title"]: extract_content(section["blocks"][0]["data"])
                            for section in page["sections"]
                        }

                    elif title == "Équipements extérieurs":
                        return_value["equippement_extérieur"] = {}

                        for section in page["sections"]:
                            titre = section["title"]
                            if "Piscine" in titre or "piscine" in titre:
                                data = section["blocks"][0]["data"]
                                contenu = extract_content(data)

                                # Use robust parser
                                pool_parsed = parse_pool_instructions(contenu["text"])

                                return_value["instruction_ouverture_piscine"] = {
                                    "text": pool_parsed.get("ouverture", ""),
                                    "images": contenu.get("images", []),
                                    "videos": contenu.get("videos", [])
                                }

                                return_value["instruction_fermeture_piscine"] = {
                                    "text": pool_parsed.get("fermeture", ""),
                                    "images": contenu.get("images", []),
                                    "videos": contenu.get("videos", [])
                                }

                            else:
                                data = section["blocks"][0]["data"]
                                contenu = extract_content(data)
                                return_value["equippement_extérieur"][titre] = contenu

                    elif title == "Recommandations":
                        reco_list = page["sections"][0]["blocks"][0]["data"]
                        return_value["recommandation"] = {
                            item["name"]: item for item in reco_list
                        }

                    elif title == "Instructions de départ":
                        import re
                        v = extract_content(
                            page["sections"][0]["blocks"][0]["data"]["content"]
                        )
                        v["text"] =  re.sub(r'^[A-ZÉÈÀÙÂÊÎÔÛÇ\s]+$', lambda m: f"</p><h2 style=\"margin-top: 1rem\">{m.group().strip()}</h2><p>", v["text"], flags=re.MULTILINE)
                        v["text"] = v["text"].replace("\n", " ")
                        return_value["instruction_depart"] = v

                found_target = True


        except Exception as e:
            print(f"Error parsing log: {e}")

    if not found_target:
        print("\nNo URLs containing 'makemytrip' were found.")

    driver.quit()
    return return_value




try:
    import requests
except ImportError:
    print("Erreur : 'requests' non installé. Lancez : pip install requests")
    sys.exit(1)

TOKEN   = os.environ.get("AIRTABLE_TOKEN", "pateBMBdl8UARzfUe.966a1390153383e1bd6dd3a7452dab235d0f04f6fd56cf499201964ce837acce")
BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "appLWhCKR6pEGE02s")

T_MAISON       = "Maison"
T_LISTEMAISON       = "ListeMaison"
T_PROPRIETAIRE = "Proprietaire"
T_LIVRET       = "Livret"
T_PISCINE      = "Piscine"
T_DISPOSITION  = "DispositionMaison"
T_EQUIPEMENT   = "equipement"
T_RECOMMANDATION   = "Recommandation"
T_WORKFLOW     = "WorkFlowBreezeway"
T_CONDITION    = "ConditionLocation"

NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}

SECTION_KEYWORDS = [
    ("accès à la maison",          "ACCES_MAISON"),
    ("fonctionnement de la maison","FONCTIONNEMENT"),
    ("fonctionnement",             "FONCTIONNEMENT"),
    ("gestion des locs",           "GESTION"),
    ("équipements bébés",          "BEBES"),
    ("known issues",               "ISSUES"),
    ("propriétaires",              "PROPRIETAIRES"),
    ("process ménage",             "PROCESS"),
    ("récap",                      "RECAP"),
    ("maison",                     "MAISON"),
    ("accès",                      "ACCES"),
]

def detect_section(text: str) -> str | None:
    t = text.strip().lower()
    t = t.strip("*").strip()
    for keyword, section in SECTION_KEYWORDS:
        if keyword in t:
            return section
    return None

def _parse_xml_bytes(xml_bytes):
    root = ET.fromstring(xml_bytes)
    body = root.find("w:body", NS)
    if body is None:
        return []
    contents = []
    for child in body:
        tag = child.tag
        if tag == f'{{{NS["w"]}}}p':
            text = "".join(n.text or "" for n in child.findall(".//w:t", NS))
            if text.strip():
                contents.append(("paragraph", text.strip()))
        elif tag == f'{{{NS["w"]}}}tbl':
            table = []
            for row in child.findall("w:tr", NS):
                cells = [
                    "".join(n.text or "" for n in cell.findall(".//w:t", NS))
                    for cell in row.findall("w:tc", NS)
                ]
                table.append(cells)
            contents.append(("table", table))
    return contents


def extract_contents(path: str):
    """Retourne une liste de ('paragraph', str) | ('table', list[list[str]])."""
    try:
        from docx import Document
        doc = Document(path)
        contents = []
        # python-docx iterates paragraphs and tables in document order via body._element
        from docx.oxml.ns import qn
        body = doc.element.body
        for child in body:
            tag = child.tag
            if tag == qn("w:p"):
                text = "".join(run.text or "" for run in child.findall(f".//{qn('w:t')}"))
                if text.strip():
                    contents.append(("paragraph", text.strip()))
            elif tag == qn("w:tbl"):
                rows = []
                for tr in child.findall(f".//{qn('w:tr')}"):
                    cells = []
                    for tc in tr.findall(f"{qn('w:tc')}"):
                        cell_text = "".join(t.text or "" for t in tc.findall(f".//{qn('w:t')}"))
                        cells.append(cell_text.strip())
                    rows.append(cells)
                contents.append(("table", rows))
        return contents
    except ImportError:
        pass

    # Fallback: raw XML
    with zipfile.ZipFile(path, "r") as z:
        return _parse_xml_bytes(z.read("word/document.xml"))



def first_email(text: str) -> str:
    m = re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", text)
    return m.group(0) if m else ""

def first_phone(text: str) -> str:
    m = re.search(r"(?:(?:\+33|0)[1-9])(?:[\s.\-]?\d{2}){4}", text)
    return m.group(0) if m else ""

def first_url(text: str) -> str:
    m = re.search(r"https?://\S+", text)
    return m.group(0) if m else ""

def table_to_dict(table_rows) -> dict:
    """Transforme un tableau 2 colonnes (clé|valeur) en dict (clé en minuscule)."""
    d = {}
    for row in table_rows:
        if len(row) >= 2 and row[0].strip():
            d[row[0].strip().lower()] = row[1].strip()
    return d

def kv_get(d: dict, *keywords) -> str:
    """Cherche la première clé contenant l'un des mots-clés."""
    for kw in keywords:
        for k, v in d.items():
            if kw in k:
                return v
    return ""


def extract_youtube_urls(videos: list) -> list:
    """Safely extract YouTube URLs from a mixed list"""
    if not videos:
        return []
    return [
        v for v in videos
        if isinstance(v, str) and "youtube" in v.lower()
    ]


def getDataExcel(id_m):
    import pandas as pd
    df = pd.read_excel("data/responsable.xlsx", usecols=[0,2,3])
    #print(df)
    find = df.loc[df['id'] == id_m]
    #print(find)
    res = find["Responsable"].iloc[0].split(" ")
    eq = find["Équipe centrale"].iloc[0].split(" ")
    return (eq[0], res[0])




def map_equipement_category(raw_cat: str) -> list:
    cat = raw_cat.lower()
    if any(k in cat for k in ("extérieur", "exterieur", "jardin", "piscine", "plancha", "barbecue")):
        return ["extérieur"]
    if any(k in cat for k in ("intérieur", "interieur", "cuisine", "billard", "salon")):
        return ["intérieur"]
    if "alarme" in cat:
        return ["alarme"]
    if any(k in cat for k in ("wifi", "internet", "adsl", "fibre")):
        return ["wifi"]
    if any(k in cat for k in ("fonctionnement", "chauffage", "électricité", "electricite", "eau", "gaz")):
        return ["fonctionnement"]
    return ["autre"]


def _clean(s: str) -> str:
    """Nettoie les espaces excessifs."""
    return re.sub(r"\s+", " ", s).strip()


def parse_and_map_data(contents: list) -> dict:
    data = {
        "Nom Maison":                 "",
        "Adresse":                    "",
        "Lien Maps":                  "",
        "Instruction Acces Externe":  "",   # instructions pour les clients
        "Instruction Acces Interne":  "",   # notes internes (badge, codes…)
        "Portail":                    "",
        "Porte Entree":               "",
        "Boite Cle":                  "",
        "Jeux de Cle":                "",
        "Alarme Interne":             "",
        "Point Attention":            "",
        "Proprietaire Nom":           "",
        "Proprietaire Prenom":        "",
        "Proprietaire Email":         "",
        "Proprietaire Tel":           "",
        "Proprietaire Info":          "",
        # ── Gestion ──────────────────────────────────────────
        "Whatsapp":                   "",
        "Titulaire Annonce":          "",
        "Proprietaire Co Hote":       "",
        "Grille Prix":                "",
        "Periodes Bloquer":           "",
        # ── Process ──────────────────────────────────────────
        "Check In":                   "",
        "Check Out":                  "",
        "Delai Menage":               "",
        # ── Fonctionnement ───────────────────────────────────
        "Chauffage":                  "",
        "Electricite":                "",
        "Internet":                   "",
        "Wifi SSID":                  "",
        "Wifi Mdp":                   "",
        "Eau Chaude":                 "",
        "Poubelle":                   "",
        "Cheminee":                   "",
        "Jardin":                     "",
        "Linge":                      "",
        "Gaz Cuisine":                "",
        "Gaz Plancha":                "",
        "Frigo":                      "",
        "Equipements Cuisine":        "",
        "Equipements Interieurs":     "",
        "Equipements Exterieurs":     "",
        "Fenetre Volet":              "",
        "Espaces Prives":             "",
        # ── Piscine ──────────────────────────────────────────
        "Piscine Raw":                "",
        # ── Bébés ────────────────────────────────────────────
        "Bebes Raw":                  [],   # lignes brutes du tableau
        # ── Bain nordique ────────────────────────────────────
        "Bain Nordique":              "",
        # ── Listes ───────────────────────────────────────────
        "Pieces":      [],   # {Piece, Description, Etage, Type}
        "Issues":      [],   # {Categorie, Probleme, Statut}
        "Equipements": [],   # {Nom, Description, Categorie}
    }

    current_section = None

    _acces_ext_lines = []

    for item_type, item in contents:

        # ── Détection de section ─────────────────────────────
        if item_type == "paragraph":
            sec = detect_section(item)
            if sec:
                current_section = sec

            # Nom de la maison (ex: "M7 - La Bergerie du Vexin")
            if not data["Nom Maison"] and re.match(r"M\d+\s*[-–]", item):
                data["Nom Maison"] = _clean(re.split(r"[-–]", item, maxsplit=1)[-1])

            # Lien Google Maps
            if "maps" in item.lower() and not data["Lien Maps"]:
                data["Lien Maps"] = first_url(item) or item.strip()

            # Paragraphes d'accès externe
            if current_section == "ACCES":
                if not any(
                    kw in item.lower()
                    for kw in ("récap", "recap", "accès à la maison", "fonctionnement", "maps.app")
                ):
                    _acces_ext_lines.append(item)

        # ── Tables ───────────────────────────────────────────
        elif item_type == "table":
            kv = table_to_dict(item)

            # ── ACCES (adresse / alarme) ──────────────────────
            if current_section in ("ACCES", "RECAP"):
                data["Adresse"]        = data["Adresse"]        or kv_get(kv, "adresse")
                data["Alarme Interne"] = data["Alarme Interne"] or kv_get(kv, "alarme")

            # ── PROCESS ──────────────────────────────────────
            if current_section == "PROCESS":
                data["Check In"]      = data["Check In"]     or kv_get(kv, "check in", "checkin")
                data["Check Out"]     = data["Check Out"]    or kv_get(kv, "checkout", "check out")
                data["Delai Menage"]  = data["Delai Menage"] or kv_get(kv, "délai", "menage", "ménage")

            # ── GESTION ──────────────────────────────────────
            if current_section == "GESTION":
                data["Whatsapp"]          = data["Whatsapp"]          or kv_get(kv, "whatsapp")
                data["Titulaire Annonce"] = data["Titulaire Annonce"] or kv_get(kv, "titulaire")
                data["Proprietaire Co Hote"] = data["Proprietaire Co Hote"] or kv_get(kv, "co-hôte", "co hote", "cohote")
                data["Grille Prix"]       = data["Grille Prix"]       or kv_get(kv, "grille")
                data["Periodes Bloquer"]  = data["Periodes Bloquer"]  or kv_get(kv, "période", "periode", "bloquer")

            # ── PROPRIETAIRES ────────────────────────────────
            if current_section == "PROPRIETAIRES":
                raw_nom   = kv_get(kv, "nom")
                raw_tel   = kv_get(kv, "téléphone", "telephone", "tel")
                raw_email = kv_get(kv, "email")
                raw_info  = kv_get(kv, "enregistrement", "rib", "numéro")
                if raw_nom and not data["Proprietaire Nom"]:
                    full = raw_nom.split(":")[-1].strip()
                    parts = full.split(" ", 1)
                    data["Proprietaire Prenom"] = parts[0] if parts else ""
                    data["Proprietaire Nom"]    = parts[1] if len(parts) > 1 else full
                if raw_tel and not data["Proprietaire Tel"]:
                    data["Proprietaire Tel"] = first_phone(raw_tel) or raw_tel.split(":")[-1].strip()
                if raw_email and not data["Proprietaire Email"]:
                    src = raw_email.split("->")[-1] if "->" in raw_email else raw_email
                    data["Proprietaire Email"] = first_email(src)
                if raw_info and not data["Proprietaire Info"]:
                    data["Proprietaire Info"] = raw_info

            # ── MAISON (pièces) ───────────────────────────────
            if current_section == "MAISON":
                current_etage = ""
                for row in item:
                    if not row:
                        continue
                    label = row[0].strip()
                    label_low = label.lower()
                    # Sous-titre d'étage
                    if len(row) == 1 or (len(row) >= 2 and not row[1].strip()):
                        if any(k in label_low for k in ("rdc", "étage", "r+", "rez", "annexe", "principale", "secondaire")):
                            current_etage = label
                            continue
                    # Pièce avec description
                    if len(row) >= 2:
                        PIECE_KEYS = ("chambre", "sdb", "salle de bain", "salon", "wc",
                                      "cuisine", "mezzanine", "déboté", "debot",
                                      "biblioth", "couloir", "bureau")
                        if any(k in label_low for k in PIECE_KEYS):
                            if "chambre" in label_low:
                                ptype = "Chambre"
                            elif any(k in label_low for k in ("sdb", "salle de bain")):
                                ptype = "Salle de bain"
                            elif "salon" in label_low:
                                ptype = "Salon"
                            elif "cuisine" in label_low:
                                ptype = "Cuisine"
                            elif "wc" in label_low:
                                ptype = "WC"
                            else:
                                ptype = "Autre"
                            data["Pieces"].append({
                                "Piece":       label,
                                "Description": row[1].strip(),
                                "Etage":       current_etage,
                                "Type":        ptype,
                            })

            # ── BEBES ─────────────────────────────────────────
            if current_section == "BEBES":
                for row in item:
                    if any(k in (row[0].lower() if row else "") for k in ("bébé", "bebe", "lit", "chaise")):
                        data["Bebes Raw"].append(row)

            # ── ACCES_MAISON ──────────────────────────────────
            if current_section == "ACCES_MAISON":
                data["Portail"]        = data["Portail"]        or kv_get(kv, "portail")
                data["Jeux de Cle"]    = data["Jeux de Cle"]    or kv_get(kv, "clés", "cles")
                data["Alarme Interne"] = data["Alarme Interne"] or kv_get(kv, "alarme")
                data["Boite Cle"]      = data["Boite Cle"]      or kv_get(kv, "boîte à clés", "boite a cles", "boîte")

            # ── FONCTIONNEMENT ────────────────────────────────
            if current_section == "FONCTIONNEMENT":
                data["Chauffage"]             = data["Chauffage"]             or kv_get(kv, "chauffage")
                data["Electricite"]           = data["Electricite"]           or kv_get(kv, "électricité", "electricite", "tableau")
                data["Piscine Raw"]           = data["Piscine Raw"]           or kv_get(kv, "piscine")
                data["Poubelle"]              = data["Poubelle"]              or kv_get(kv, "poubelle")
                data["Cheminee"]              = data["Cheminee"]              or kv_get(kv, "cheminée", "cheminee", "poêle")
                data["Jardin"]                = data["Jardin"]                or kv_get(kv, "jardin")
                data["Linge"]                 = data["Linge"]                 or kv_get(kv, "linge")
                data["Gaz Cuisine"]           = data["Gaz Cuisine"]           or kv_get(kv, "gaz - cuisine", "gaz cuisine", "gaz\xa0cuisine")
                data["Gaz Plancha"]           = data["Gaz Plancha"]           or kv_get(kv, "gaz - plancha", "gaz plancha", "gaz\xa0plancha")
                data["Internet"]              = data["Internet"]              or kv_get(kv, "internet", "wifi", "adsl", "fibre")
                data["Frigo"]                 = data["Frigo"]                 or kv_get(kv, "frigo", "congélateur", "congelateur")
                data["Equipements Cuisine"]   = data["Equipements Cuisine"]   or kv_get(kv, "équipements de cuisine", "equipements cuisine", "équipements cuisine")
                data["Equipements Interieurs"]= data["Equipements Interieurs"]or kv_get(kv, "équipements intérieurs", "equipements interieurs")
                data["Equipements Exterieurs"]= data["Equipements Exterieurs"]or kv_get(kv, "équipements extérieurs", "equipements exterieurs")
                data["Fenetre Volet"]         = data["Fenetre Volet"]         or kv_get(kv, "fenêtre", "fenetre", "volet")
                data["Espaces Prives"]        = data["Espaces Prives"]        or kv_get(kv, "espaces privés", "espaces prives", "espace privé")
                data["Bain Nordique"]         = data["Bain Nordique"]         or kv_get(kv, "bain nordique", "jacuzzi", "spa", "bain")
                data["Eau Chaude"]            = data["Eau Chaude"]            or kv_get(kv, "eau chaude")

                # WiFi SSID / mdp depuis le champ Internet
                inet = data["Internet"]
                if inet:
                    m_ssid = re.search(r"(?:ssid|box|livebox)[^\n:]*[:\s]+([^\n]+)", inet, re.I)
                    m_mdp  = re.search(r"(?:mdp|mot de passe|password|clé wifi)[^\n:]*[:\s]+([^\n]+)", inet, re.I)
                    if m_ssid and not data["Wifi SSID"]:
                        data["Wifi SSID"] = m_ssid.group(1).strip()
                    if m_mdp and not data["Wifi Mdp"]:
                        data["Wifi Mdp"] = m_mdp.group(1).strip()

                # Équipements → table equipement
                for cat_label, cat_key, cat_mapped in [
                    ("Équipements cuisine",    "Equipements Cuisine",    "intérieur"),
                    ("Équipements intérieurs", "Equipements Interieurs", "intérieur"),
                    ("Équipements extérieurs", "Equipements Exterieurs", "extérieur"),
                    ("Chauffage",              "Chauffage",              "fonctionnement"),
                    ("Internet / Wifi",        "Internet",               "wifi"),
                    ("Piscine",                "Piscine Raw",            "extérieur"),
                    ("Bain nordique",          "Bain Nordique",          "extérieur"),
                    ("Gaz cuisine",            "Gaz Cuisine",            "fonctionnement"),
                    ("Gaz plancha",            "Gaz Plancha",            "fonctionnement"),
                    ("Cheminée",               "Cheminee",               "intérieur"),
                    ("Jardin",                 "Jardin",                 "extérieur"),
                    ("Frigo / Congélateur",    "Frigo",                  "intérieur"),
                    ("Poubelle",               "Poubelle",               "fonctionnement"),
                ]:
                    val = data.get(cat_key, "")
                    if val:
                        # Éviter les doublons
                        if not any(e["Nom"] == cat_label for e in data["Equipements"]):
                            data["Equipements"].append({
                                "Nom":        cat_label,
                                "Description": val,
                                "Categorie":  cat_mapped,
                            })

            # ── ISSUES ────────────────────────────────────────
            if current_section == "ISSUES":
                for row in item:
                    if len(row) < 3:
                        continue
                    if row[0].strip().lower() in ("catégorie", "categorie", ""):
                        continue
                    data["Issues"].append({
                        "Categorie": row[0].strip(),
                        "Probleme":  row[1].strip(),
                        "Statut":    row[2].strip(),
                    })

    data["Instruction Acces Externe"] = "\n".join(_acces_ext_lines).strip()

    # Instruction accès interne = portail + boîte + alarme
    _internal = []
    if data["Portail"]:    _internal.append("PORTAIL\n" + data["Portail"])
    if data["Boite Cle"]:  _internal.append("BOÎTE À CLÉS\n" + data["Boite Cle"])
    if data["Alarme Interne"]: _internal.append("ALARME\n" + data["Alarme Interne"])
    data["Instruction Acces Interne"] = "\n\n".join(_internal)

    return data



HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {TOKEN}",
}

def urls_to_attachments(urls: list) -> list:
    return [{"url": url} for url in urls if url and isinstance(url, str)]


def _url(table: str) -> str:
    return f"https://api.airtable.com/v0/{BASE_ID}/{requests.utils.quote(table)}"


def airtable_create(table: str, fields: dict) -> str | None:
    resp = requests.post(_url(table), headers=HEADERS, json={"fields": fields})
    if resp.status_code in (200, 201):
        rid = resp.json().get("id")
        print(f"   ✅ [{table}] créé → {rid}")
        return rid
    print(f"   ❌ [{table}] erreur {resp.status_code} : {resp.text}")
    return None


def find_equivalent_description_interne_equipement(objet, all_equip):
    for e in all_equip:
        parts  = e["Nom"].split("—", 1)
        nom    = parts[0].strip()

        if objet in nom or objet in e["Description"]:
            return (e["Description"], parts[1].strip() if len(parts) > 1 else e.get("Checklist", ""))

    return ("", "")

def airtable_create_batch(table: str, records: list) -> list:
    url = _url(table)
    ids = []
    for i in range(0, len(records), 10):
        batch = [{"fields": r} for r in records[i:i + 10]]
        resp  = requests.post(url, headers=HEADERS, json={"records": batch})
        if resp.status_code in (200, 201):
            batch_ids = [r["id"] for r in resp.json().get("records", [])]
            ids.extend(batch_ids)
            print(f"  [{table}] batch {i // 10 + 1} → {len(batch_ids)} enregistrement(s)")
        else:
            print(f"   [{table}] batch {i // 10 + 1} erreur {resp.status_code} : {resp.text}")
    return ids


def airtable_patch(table: str, record_id: str, fields: dict):
    url  = f"{_url(table)}/{record_id}"
    resp = requests.patch(url, headers=HEADERS, json={"fields": fields})
    if resp.status_code == 200:
        print(f"  [{table}] {record_id} mis à jour")
    else:
        print(f"   [{table}] patch {record_id} erreur {resp.status_code} : {resp.text}")


def airtable_find_by_field(table: str, field: str, value: str) -> str | None:
    """Cherche un enregistrement par valeur de champ, retourne son ID."""
    params = {"filterByFormula": f"{{{field}}}='{value}'"}
    resp   = requests.get(_url(table), headers=HEADERS, params=params)
    if resp.status_code == 200:
        records = resp.json().get("records", [])
        if records:
            return records[0]["id"]
    return None


def find_or_create_proprietaire(data: dict) -> str | None:
    email = data["Proprietaire Email"]
    if email:
        rid = airtable_find_by_field(T_PROPRIETAIRE, "Email", email)
        if rid:
            print(f"   ℹ️  [Proprietaire] déjà existant → {rid}")
            return rid
    return airtable_create(T_PROPRIETAIRE, {
        "Nom":                data["Proprietaire Nom"],
        "Prenom":             data["Proprietaire Prenom"],
        "Email":              data["Proprietaire Email"],

        "Telephone":          data["Proprietaire Tel"],
        "Groupe whatsapp":    data["Whatsapp"],
        "InfoComplémentaire": data["Proprietaire Info"],
    })


def replace_beacons_in_text(text: str, images: list, videos: list) -> str:
    """
    Replace $i0, $i1... and $v0, $v1... placeholders with actual URLs.
    YouTube videos are kept as URLs (for JS to detect), non-YouTube videos too.
    """
    if not text:
        return ""

    result = text

    for idx, img_url in enumerate(images):
        beacon = f"$i{idx}"
        if beacon in result:
            result = result.replace(beacon, f"\n{img_url}\n")

    for idx, vid_url in enumerate(videos):
        beacon = f"$v{idx}"
        if beacon in result:
            result = result.replace(beacon, f"\n{vid_url}\n")

    result = re.sub(r'\n{3,}', '\n\n', result)
    return result.strip()


def parse_pool_section(text: str, keyword: str) -> str:
    """Extract section text after a keyword (fermeture/ouverture)"""
    pattern = rf'(?:{keyword}|{keyword.capitalize()})\s*[:\-]?\s*(.+?)(?:\n\n|ouverture|Ouverture|fermeture|Fermeture|$)'
    match = re.search(pattern, text, re.DOTALL | re.I)
    return match.group(1).strip() if match else ""


def format_youtube_field(videos: list) -> str | list:
    """
    Format YouTube videos for Airtable.
    Returns first URL as string (for text/URL fields).
    Modify to return list if your field supports multiple values.
    """
    yt_urls = [v for v in videos if "youtube" in v.lower()]
    return yt_urls[0] if yt_urls else None

def parse_pool_instructions(raw_text: str) -> dict:
    """Robust parser for pool instructions with NO.1/NO.2 format"""
    sections = {"ouverture": "", "fermeture": "", "entretien": ""}

    if not raw_text:
        return sections

    # Split by common section markers
    # Pattern: "NO.1", "NO.2", "Ouverture:", "Fermeture:", etc.
    parts = re.split(r'\n\s*(?:NO\.\d+|Ouverture|Fermeture|Entretien|Opening|Closing)\s*[:\-]?\s*', raw_text, flags=re.IGNORECASE)
    markers = re.findall(r'\n\s*(NO\.\d+|Ouverture|Fermeture|Entretien|Opening|Closing)\s*[:\-]?\s*', raw_text, flags=re.IGNORECASE)

    current_section = "ouverture"  # default
    for i, part in enumerate(parts):
        if not part.strip():
            continue
        if i > 0 and markers:
            marker = markers[i-1].lower()
            if any(k in marker for k in ["ferm", "close", "no.2"]):
                current_section = "fermeture"
            elif any(k in marker for k in ["entret", "clean", "ph"]):
                current_section = "entretien"
            else:
                current_section = "ouverture"

        if sections[current_section]:
            sections[current_section] += "\n" + part.strip()
        else:
            sections[current_section] = part.strip()

    return {k: v.strip() for k, v in sections.items() if v.strip()}

def send_to_airtable(data: dict, id_maison: str, dry_run: bool = False):
    print("\n📦 Préparation de l'envoi vers Airtable…\n")
    data_breezeway = clean_extracted_data(extract_data_breezeway())
    if dry_run:
        print("   Mode dry-run : aucune donnée ne sera envoyée.\n")
        return

    print("1️⃣  Propriétaire")
    prop_id = find_or_create_proprietaire(data)


    point_attention = ""
    list_media = []  # For Photos attachment field
    list_youtube = []  # For separate YouTube tracking if needed

    for title, content in data_breezeway.get("point_attention", {}).items():
        point_attention += f"<h1>{title}</h1>\n{content.get('text', '')}\n"

        images = content.get("images", []) or []
        videos = content.get("videos", []) or []

        list_media.extend([img for img in images if isinstance(img, str)])

        # ✅ Collect YouTube URLs separately
        youtube_urls = [v for v in videos if isinstance(v, str) and "youtube" in v.lower()]
        list_youtube.extend(youtube_urls)



    print("\nMaison")
    maison_fields = {
        "IdMaison":                  id_maison,
        "Adresse":                   data_breezeway["adresse"],
        "Description":               data_breezeway["html_bienvenue"],
        "AlarmeInterne":             data["Alarme Interne"],
        "InstructionAccesInterne":   data["Instruction Acces Interne"],
        "InstructionAccesExterne":   data["Instruction Acces Externe"],
        "InstructionDepart":      data_breezeway["instruction_depart"]["text"],
        "ResponsableCentrale": getDataExcel(id_maison)[0],

        "Proprietaire": [prop_id],
        "ResponsableZone" : getDataExcel(id_maison)[1],
        "Portail":                   data["Portail"],
        "BoiteCle":                  data["Boite Cle"],
        "JeuxDeCle":               data["Jeux de Cle"],
        "mediaPointAttention":       urls_to_attachments(list_media),
        "PointAttention":           point_attention
    }

    maison_id = airtable_create(T_MAISON, maison_fields)

    print("\n ListeMaison")
    liste_maison_fields = {
        "IdMaison":                  id_maison,
        "NomMaison":              data["Nom Maison"],
        "ImageMaison":           urls_to_attachments([data_breezeway["photo_maison"]]),
    }
    liste_maison = airtable_create(T_LISTEMAISON, liste_maison_fields)


    print("\n  Livret")
    livret_fields = {"IdMaison": data["Nom Maison"]}
    if maison_id:
        livret_fields["Maison"] = [maison_id]
    livret_id = airtable_create(T_LIVRET, livret_fields)
    if maison_id and livret_id:
        airtable_patch(T_MAISON, maison_id, {"Livret": [livret_id]})

    print("\n  Piscine")
    raw_pool_text = data.get("Piscine Raw", "")

    desinfectant = (["Sel"] if "sel" in raw_pool_text.lower()
                    else ["Chlore"] if "chlore" in raw_pool_text.lower()
                    else ["Autre"])

    # Extract Chauffage
    chauffage = []
    if any(w in raw_pool_text.lower() for w in ["chauffée", "pompe à chaleur", "pac ", "chauffage"]):
        chauffage.append("Pompe à chaleur")

    ouv_data = data_breezeway.get("instruction_ouverture_piscine", {})
    ferm_data = data_breezeway.get("instruction_fermeture_piscine", {})

    ouv_text = ouv_data.get("text", "") if isinstance(ouv_data, dict) else ""
    ferm_text = ferm_data.get("text", "") if isinstance(ferm_data, dict) else ""

    pool_images = []
    if isinstance(ouv_data, dict):
        pool_images.extend(ouv_data.get("images", []) or [])
    if isinstance(ferm_data, dict):
        pool_images.extend(ferm_data.get("images", []) or [])

    # Also check extérieur equipment for pool-related items
    ext_equip = data_breezeway.get("equippement_extérieur", {})
    for key, val in ext_equip.items():
        if "piscine" in key.lower() or "pool" in key.lower():
            if isinstance(val, dict):
                pool_images.extend(val.get("images", []) or [])

    # Remove duplicates while preserving order
    pool_images = list(dict.fromkeys([img for img in pool_images if isinstance(img, str)]))

    piscine_fields = {
        "Name": data["Nom Maison"] + " - Piscine",
        "InstructionOuverture": ouv_text,
        "InstructionFermeture": ferm_text,
        "InstructionEntretien": raw_pool_text or ouv_text,
        "InstructionClient": raw_pool_text,
        "Desinfectant": desinfectant,
    }

    if chauffage:
        piscine_fields["Chauffage"] = chauffage

    if pool_images:
        piscine_fields["Photo"] = urls_to_attachments(pool_images)

    pool_videos = []
    if isinstance(ouv_data, dict):
        pool_videos.extend(ouv_data.get("videos", []) or [])
    if isinstance(ferm_data, dict):
        pool_videos.extend(ferm_data.get("videos", []) or [])

    youtube_pool_urls = [v for v in pool_videos if isinstance(v, str) and "youtube" in v.lower()]
    if youtube_pool_urls:
        piscine_fields["lienYoutube"] = youtube_pool_urls[0]

    # Detect product type
    if "auto" in raw_pool_text.lower() or "régulé" in raw_pool_text.lower():
        piscine_fields["Produit"] = ["Autogérée"]
    elif "manuelle" in raw_pool_text.lower() or "manuel" in raw_pool_text.lower():
        piscine_fields["Produit"] = ["manuelle"]

    if maison_id:
        piscine_fields["Maison"] = [maison_id]
    piscine_id = airtable_create(T_PISCINE, piscine_fields)
    if maison_id and piscine_id:
        airtable_patch(T_MAISON, maison_id, {"Piscine": [piscine_id]})



    if data["Pieces"]:
        print(f"\n  DispositionMaison ({len(data['Pieces'])} pièces)")
        pieces_records = []
        for p in data["Pieces"]:
            rec = {
                "Piece":       p["Piece"],
                "Description": p["Description"],
                "Etage":       p["Etage"],
                "type":        p["Type"],
            }
            if maison_id:
                rec["Maison"] = [maison_id]
            pieces_records.append(rec)
        airtable_create_batch(T_DISPOSITION, pieces_records)
    else:
        print("\n  DispositionMaison — aucune pièce détectée")


    all_equip = list(data["Equipements"])
   ## Known issues → équipements catégorie "autre" + note statut
   #for issue in data["Issues"]:
   #    label = f"{issue['Categorie']} — {issue['Probleme'][:80]}"
   #    all_equip.append({
   #        "Nom":        label,
   #        "Description": f"{issue['Probleme']}\nStatut : {issue['Statut']}",
   #        "Categorie":  "autre",
   #        "Checklist":  issue["Probleme"][:80],
   #    })

   #if all_equip:
   #    print(f"\n6️⃣  Équipements ({len(all_equip)} entrées)")
   #    equip_records = []
   #    for e in all_equip:
   #        parts  = e["Nom"].split("—", 1)
   #        nom    = parts[0].strip()
   #        cl_val = parts[1].strip() if len(parts) > 1 else e.get("Checklist", "")
   #        rec = {
   #            "NomEquipement":      nom,
   #            "descriptionInterne": e["Description"],
   #            "Catégorie":          map_equipement_category(e["Categorie"]),
   #            "CheckListMenage":    cl_val,
   #        }
   #        if livret_id:
   #            rec["Livret"] = [livret_id]
   #        equip_records.append(rec)
   #    airtable_create_batch(T_EQUIPEMENT, equip_records)
   #else:
   #    print("\n6️⃣  Équipements — aucun détecté")
    print("🔧 Processing equipments...")
    equip_records = []
    processed_names = set()  # ✅ Track names to avoid duplicates

    def extract_youtube_urls(videos: list) -> list:
        """Extract YouTube URLs from a list, safely handling None/non-string values"""
        if not videos:
            return []
        return [v for v in videos if isinstance(v, str) and "youtube" in v.lower()]

    def process_equipment(
        equip_name: str,
        equip_data: dict,
        categorie: str,
        livret_id: str | None,
        all_equip: list
    ) -> dict | None:
        """Process one equipment item — beacons stay, video URLs injected for JS"""

        if equip_name in processed_names:
            print(f"   ⏭️  Skipping duplicate: {equip_name}")
            return None
        processed_names.add(equip_name)

        equivalent_data = find_equivalent_description_interne_equipement(equip_name, all_equip)

        images = [img for img in (equip_data.get("images") or []) if isinstance(img, str)]
        videos = [vid for vid in (equip_data.get("videos") or []) if isinstance(vid, str)]
        text = equip_data.get("text", "") or ""

        # Separate YouTube from other videos
        youtube_urls = [v for v in videos if "youtube" in v.lower()]
        other_videos = [v for v in videos if v not in youtube_urls]

        # ✅ INJECT video URLs into text at beacon positions for JavaScript to detect
        # This keeps beacons AND adds URLs so JS has something to work with
        description_with_urls = text
        for idx, vid_url in enumerate(videos):
            beacon = f"$v{idx}"
            if beacon in description_with_urls:
                # Insert URL on its own line after the beacon: "$v0\nhttps://youtube..."
                description_with_urls = description_with_urls.replace(
                    beacon,
                    f"{beacon}\n{vid_url}"
                )

        record = {
            "NomEquipement": equip_name,
            "Livret": [livret_id] if livret_id else [],
            "Catégorie": [categorie],
            "Photos": urls_to_attachments(images),  # Images only
            "descriptionLivret": description_with_urls,  # ✅ Beacons + injected URLs
            "descriptionInterne": equivalent_data[0],
            "CheckListMenage": equivalent_data[1] or equivalent_data[0]
        }

        # ✅ Also send YouTube URL separately for fallback/alternative rendering
        if youtube_urls:
            record["lienYoutube"] = youtube_urls[0]

        return record

    # ── Process INTÉRIEUR equipment ──────────────────────────────────
    print("   📦 Processing intérieur equipment...")
    for equip_name, equip_data in data_breezeway.get("equippement_intérieur", {}).items():
        record = process_equipment(
            equip_name=equip_name,
            equip_data=equip_data,
            categorie="intérieur",
            livret_id=livret_id,
            all_equip=all_equip
        )
        if record:  # Only append if not duplicate
            equip_records.append(record)
            print(f"   ✅ intérieur: {equip_name} | imgs:{len(record['Photos'])} | yt:{'lienYoutube' in record}")

    # ── Process EXTÉRIEUR equipment ──────────────────────────────────
    print("   📦 Processing extérieur equipment...")
    for equip_name, equip_data in data_breezeway.get("equippement_extérieur", {}).items():
        record = process_equipment(
            equip_name=equip_name,
            equip_data=equip_data,
            categorie="extérieur",  # ✅ Fixed: was "intérieur" before
            livret_id=livret_id,
            all_equip=all_equip
        )
        if record:  # Only append if not duplicate
            equip_records.append(record)
            print(f"   ✅ extérieur: {equip_name} | imgs:{len(record['Photos'])} | yt:{'lienYoutube' in record}")

    # ── Add WiFi equipment (special case: no media, just credentials) ─
    if "wifi" not in processed_names:
        equip_records.append({
            "NomEquipement": "wifi",
            "Livret": [livret_id] if livret_id else [],
            "Catégorie": ["wifi"],
            "descriptionInterne": f"{data_breezeway.get('name_wifi', '')}\n{data_breezeway.get('mdp_wifi', '')}",
            "descriptionLivret": f"{data_breezeway.get('name_wifi', '')}\n{data_breezeway.get('mdp_wifi', '')}"
        })
        print(f"   ✅ wifi: credentials added")
        processed_names.add("wifi")

    # ── Final deduplication safety net (by NomEquipement) ─────────────
    # In case any duplicates slipped through
    seen = {}
    deduped_records = []
    for rec in equip_records:
        name = rec["NomEquipement"]
        if name not in seen:
            seen[name] = True
            deduped_records.append(rec)
        else:
            print(f"   ⚠️  Removed duplicate: {name}")

    equip_records = deduped_records

    # ── Send to Airtable ─────────────────────────────────────────────
    print(f"📦 Total unique equipment records to send: {len(equip_records)}")

    if dry_run:
        print("   [DRY-RUN] Would send equipment records (none sent)")
        # Optional: show sample
        if equip_records:
            print("\n🔍 Sample record:")
            import json
            print(json.dumps(equip_records[0], indent=2, ensure_ascii=False, default=str))
    else:
        if equip_records:
            airtable_create_batch(T_EQUIPEMENT, equip_records)
        else:
            print("   ⚠️  No equipment records to send")


    print("Recommandation")
    reco_records = []
    for equip_name, equip_data in data_breezeway["recommandation"].items():
        reco_records.append({
            "NomRecommandation": equip_data["name"],
            "Description": equip_data["summary"],
            "Score": equip_data["rating"],
            "localisation": str(equip_data["latitude"]) + ";" + str(equip_data["longitude"]),
            "google_place_id": equip_data["google_place_id"],
            "Adress": equip_data["city"] + equip_data["formatted_address"],
            "Maison": [maison_id]
        })

    airtable_create_batch(T_RECOMMANDATION, reco_records)


    print("\n7️⃣  WorkFlowBreezeway")
    wf_records = []
    ci_raw = data["Check In"]
    co_raw = data["Check Out"]
    # Chaque ligne non vide = une tâche
    if ci_raw:
        for line in ci_raw.split("\n"):
            line = line.strip().lstrip("-•").strip()
            if line:
                rec = {"Name": line, "CICO": data_breezeway["CICO"]}
                if maison_id:
                    rec["Maison"] = [maison_id]
                wf_records.append(rec)
    else:
        for line in co_raw.split("\n"):
            line = line.strip().lstrip("-•").strip()
            if line:
                rec = {"Name": line, "CICO": data_breezeway["CICO"]}
                if maison_id:
                    rec["Maison"] = [maison_id]
                wf_records.append(rec)

    if wf_records:
        airtable_create_batch(T_WORKFLOW, wf_records)
    else:
        print("   Aucune tâche check-in/check-out à créer")

    print("\nConditionLocation")
    cond_fields = {
        "Name": data["Nom Maison"] + " - Conditions",
    }
    if maison_id:
        cond_fields["Maison"] = [maison_id]
    # Chien : chercher dans les équipements/conditions
    _grille = data.get("Grille Prix", "").lower()
    if "chien" in _grille or "animaux" in _grille:
        cond_fields["chien"] = True
    airtable_create(T_CONDITION, cond_fields)

    print(f"\n✅  Import terminé pour « {data['Nom Maison']} » (ID: {id_maison})")



def main():
    parser = argparse.ArgumentParser(
        description="Extrait un livret .docx et l'envoie vers Airtable.")
    parser.add_argument("input_file", help="Chemin vers le fichier .docx")
    parser.add_argument("--id-maison", default="",
                        help="Identifiant de la maison (ex: M7). Demandé interactivement si absent.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Affiche les données sans envoyer vers Airtable")
    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise FileNotFoundError(f"Fichier introuvable : {args.input_file}")

    id_maison = args.id_maison or input("Entrer l'id de la maison (ex: M7, M64…) : ").strip()

    print("⏳ Extraction du document…")
    contents = extract_contents(args.input_file)

    print("🔍 Analyse et mapping…")
    extracted = parse_and_map_data(contents)


    print("\n─── Données extraites ───────────────────────────────")
    summary_fields = [
        "Nom Maison", "Adresse", "Lien Maps",
        "Proprietaire Nom", "Proprietaire Prenom",
        "Proprietaire Email", "Proprietaire Tel",
        "Whatsapp", "Titulaire Annonce",
        "Check In", "Check Out", "Delai Menage",
        "Chauffage", "Internet", "Wifi SSID",
        "Piscine Raw",
    ]
    for f in summary_fields:
        v = extracted.get(f, "")
        if v:
            print(f"  {f:30s}: {str(v)[:80]}")
    print(f"\n  Pièces         : {len(extracted['Pieces'])}")
    print(f"  Équipements    : {len(extracted['Equipements'])}")
    print(f"  Known issues   : {len(extracted['Issues'])}")
    print("─────────────────────────────────────────────────────\n")

    send_to_airtable(extracted, id_maison, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
