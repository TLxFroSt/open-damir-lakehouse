"""Construction de reference/nomenclatures.csv : libellé de chaque code des colonnes DAMIR.

Le descriptif officiel Open DAMIR n'étant pas téléchargeable, les libellés viennent de :
- nomenclatures SNDS du Health Data Hub (dépôt schema-snds, licence MPL 2.0), à jour ;
- lexique Open DAMIR 2015 (CNAM, copie du dépôt SGMAP-AGD/DAMIR) ;
- reference/complements_nomenclatures.csv : quelques codes absents des deux, chacun justifié.

Ordre de priorité choisi colonne par colonne, après comparaison des libellés sur janvier 2025 :
- codes bruts du SNDS (nature de prestation, exonération...) : SNDS d'abord, plus récent ;
- regroupements propres à Open DAMIR (tranches d'âge, spécialités regroupées...) : lexique
  d'abord. Exemple : la tranche « 70 » vaut « 70 - 79 ans » dans Open DAMIR mais
  « 70 - 74 ans » dans la table SNDS des âges, qui ne doit donc pas servir ici.

Reconstruire : uv run python -m damir.reference.nomenclatures
"""

import csv
import io
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import xlrd

REFERENCE_DIR = Path(__file__).parents[3] / "reference"
OUTPUT_FILE = REFERENCE_DIR / "nomenclatures.csv"
COMPLEMENTS_FILE = REFERENCE_DIR / "complements_nomenclatures.csv"
OUTPUT_COLUMNS = ("code_damir", "code", "libelle", "source")

# Versions figées des sources : reconstruire donne toujours le même fichier
SNDS_COMMIT = "7bfdf37a3c6792240cb27e13d5cb19199e619fbe"  # schema-snds, avril 2026
SNDS_URL = (
    "https://gitlab.com/api/v4/projects/11935694/repository/files/"
    "nomenclatures%2FORAVAL%2F{table}.csv/raw?ref=" + SNDS_COMMIT
)
LEXICON_COMMIT = "61db19a8bcb2ff3e5bec0f6451a943921118ad15"  # SGMAP-AGD/DAMIR
LEXICON_URL = (
    f"https://raw.githubusercontent.com/SGMAP-AGD/DAMIR/{LEXICON_COMMIT}/"
    "OpenDamir/documentation/Lexique%20open-DAMIR.xls"
)

LEXICON_SOURCE = "Lexique Open DAMIR 2015"
COMPLEMENT_SOURCE = "Complément du projet"


@dataclass(frozen=True)
class SndsTable:
    """Table de nomenclature SNDS : colonne du code et colonne du libellé."""

    table: str
    key: str
    label: str

    @property
    def source(self) -> str:
        return f"SNDS {self.table}"


@dataclass(frozen=True)
class LexiconSheet:
    """Onglet du lexique Open DAMIR 2015 (code en colonne A, libellé en colonne B)."""

    sheet: str

    @property
    def source(self) -> str:
        return LEXICON_SOURCE


Source = SndsTable | LexiconSheet
Labels = dict[str, str]  # code -> libellé

_REGIONS = [SndsTable("IR_REG_V", "GEO_REG_COD", "GEO_REG_LIB")]
_CATEGORIES = SndsTable("IR_CET_D", "ETB_CAT_RG1", "ETB_CR1_LIB")
_SPECIALTIES = SndsTable("IR_SPE_V", "PFS_SPE_COD", "PFS_SPE_LIB")
_ACTIVITIES = SndsTable("IR_ACT_V", "PFS_ACT_NAT", "ACT_NAT_LIB")

# Pour chaque code DAMIR : sources par ordre de priorité (le premier libellé trouvé l'emporte)
COLUMN_SOURCES: dict[str, list[Source]] = {
    # Codes bruts du SNDS : nomenclature SNDS à jour d'abord
    "PRS_NAT": [SndsTable("IR_NAT_V", "PRS_NAT", "PRS_NAT_LIB"), LexiconSheet("PRS_NAT")],
    "ASU_NAT": [SndsTable("IR_ASU_V", "ASU_NAT", "ASU_NAT_LIB"), LexiconSheet("ASU_NAT")],
    "ATT_NAT": [SndsTable("IR_ATT_V", "ATT_NAT", "ATT_NAT_LIB"), LexiconSheet("ATT_NAT")],
    "CPT_ENV_TYP": [SndsTable("IR_ENV_V", "CPT_ENV_TYP", "ENV_TYP_LIB"), LexiconSheet("CPT_ENV_TYP")],
    "EXO_MTF": [SndsTable("IR_EXO_V", "EXO_MTF", "EXO_LIB"), LexiconSheet("EXO_MTF")],
    "MTM_NAT": [SndsTable("IR_MTM_V", "MTM_NAT", "MTM_LIB"), LexiconSheet("MTM_NAT")],
    "PRS_FJH_TYP": [SndsTable("IR_FJH_V", "PRS_FJH_TYP", "PRS_FJH_LIB"), LexiconSheet("PRS_FJH_TYP")],
    "PRS_PPU_SEC": [SndsTable("IR_PPU_V", "PPU_SEC", "PPU_SEC_LIB"), LexiconSheet("PRS_PPU_SEC")],
    "DRG_AFF_NAT": [SndsTable("IR_DRG_V", "DRG_NAT", "DRG_NAT_LIB"), LexiconSheet("DRG_AFF_NAT")],
    "BEN_SEX_COD": [SndsTable("IR_SEX_V", "SEX_COD", "SEX_LIB"), LexiconSheet("BEN_SEX_COD")],
    "MDT_TYP_COD": [SndsTable("IR_MDT_V", "MDT_TYP_COD", "MDT_TYP_LIB"), LexiconSheet("MDT_TYP_COD")],
    "MFT_COD": [SndsTable("IR_MFT_V", "MFT_COD", "MFT_LIB"), LexiconSheet("MFT_COD")],
    "ETE_IND_TAA": [SndsTable("IR_TAA_V", "TAA_COD", "TAA_LIB"), LexiconSheet("ETE_IND_TAA")],
    "ETB_DCS_MCO": [SndsTable("IR_MCO_V", "GHS_DDP_MCO", "DDP_MCO_LIB")],
    "ETE_CAT_SNDS": [_CATEGORIES, LexiconSheet("ETE_CAT_SNDS")],
    "ETP_CAT_SNDS": [_CATEGORIES, LexiconSheet("ETP_CAT_SNDS")],
    "ORG_CLE_REG": _REGIONS,
    "BEN_RES_REG": _REGIONS,
    "ETE_REG_COD": _REGIONS,
    "ETP_REG_COD": _REGIONS,
    "EXE_INS_REG": _REGIONS,
    "PRE_INS_REG": _REGIONS,
    # Regroupements propres à Open DAMIR : lexique d'abord
    "AGE_BEN_SNDS": [LexiconSheet("AGE_BEN_SNDS")],
    "PSE_SPE_SNDS": [LexiconSheet("PSE_SPE_SNDS"), _SPECIALTIES],
    "PSP_SPE_SNDS": [LexiconSheet("PSE_SPE_SNDS"), _SPECIALTIES],
    "PSE_ACT_SNDS": [LexiconSheet("PSE_ACT_SNDS"), _ACTIVITIES],
    "PSP_ACT_SNDS": [LexiconSheet("PSE_ACT_SNDS"), _ACTIVITIES],
    # Présents uniquement dans le lexique (les colonnes PSP_* reprennent les onglets PSE_*)
    "CPL_COD": [LexiconSheet("CPL_COD")],
    "PRS_PDS_QCP": [LexiconSheet("PRS_PDS_QCP")],
    "PRS_REM_TYP": [LexiconSheet("PRS_REM_TYP")],
    "BEN_QLT_COD": [LexiconSheet("BEN_QLT_COD")],
    "BEN_CMU_TOP": [LexiconSheet("BEN_CMU_TOP")],
    "PSE_ACT_CAT": [LexiconSheet("PSE_ACT_CAT")],
    "PSP_ACT_CAT": [LexiconSheet("PSE_ACT_CAT")],
    "PSE_STJ_SNDS": [LexiconSheet("PSE_STJ_SNDS")],
    "PSP_STJ_SNDS": [LexiconSheet("PSE_STJ_SNDS")],
    "ETE_TYP_SNDS": [LexiconSheet("ETE_TYP_SNDS")],
    "DDP_SPE_COD": [LexiconSheet("DDP_SPE_COD")],
    # Aucune source : uniquement le fichier de compléments
    "TOP_PS5_TRG": [],
}  # fmt: skip


def normalize_code(value: object) -> str:
    """Code sous forme canonique : « 1111.0 » (cellule Excel) -> « 1111 », espaces retirés."""
    text = str(value).strip()
    try:
        number = float(text)
    except ValueError:
        return text
    return str(int(number)) if number.is_integer() else text


def _clean_label(value: object) -> str:
    """Libellé sans guillemets parasites ni espaces superflus."""
    return " ".join(str(value).replace('"', " ").split())


def parse_snds_table(content: str, table: SndsTable) -> Labels:
    """Libellés d'une table SNDS (CSV « ; ») ; en cas de code répété, le premier l'emporte."""
    labels: Labels = {}
    for row in csv.DictReader(io.StringIO(content), delimiter=";"):
        code, label = row.get(table.key) or "", row.get(table.label) or ""
        if code.strip() and label.strip():
            labels.setdefault(normalize_code(code), _clean_label(label))
    return labels


def parse_lexicon_sheet(book: xlrd.book.Book, sheet: LexiconSheet) -> Labels:
    """Libellés d'un onglet du lexique 2015 (ligne 1 : en-tête)."""
    rows = book.sheet_by_name(sheet.sheet)
    codes, labels = rows.col_values(0)[1:], rows.col_values(1)[1:]
    return {
        normalize_code(code): _clean_label(label)
        for code, label in zip(codes, labels, strict=True)
        if str(code).strip() and str(label).strip()
    }


def merge_column(
    code_damir: str,
    sources: list[tuple[str, Labels]],
    complements: Labels,
) -> list[tuple[str, str, str, str]]:
    """Lignes (code_damir, code, libellé, source) d'une colonne, sources par priorité.

    Les compléments ne servent qu'aux codes absents de toutes les sources : un complément
    qui doublonnerait une source est une erreur (le fichier de compléments reste minimal).
    """
    merged: dict[str, tuple[str, str]] = {}
    for source_name, labels in sources:
        for code, label in labels.items():
            merged.setdefault(code, (label, source_name))
    duplicated = sorted(set(complements) & set(merged))
    if duplicated:
        raise ValueError(f"{code_damir} : compléments déjà couverts par une source : {duplicated}")
    for code, label in complements.items():
        merged[code] = (label, COMPLEMENT_SOURCE)
    return [(code_damir, code, label, source) for code, (label, source) in sorted(merged.items())]


def read_complements(path: Path = COMPLEMENTS_FILE) -> dict[str, Labels]:
    """Compléments par code DAMIR, lus depuis le CSV (code_damir;code;libelle;justification)."""
    complements: dict[str, Labels] = {}
    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter=";"):
            complements.setdefault(row["code_damir"], {})[normalize_code(row["code"])] = row[
                "libelle"
            ]
    return complements


def build_rows(
    fetch_snds: Callable[[SndsTable], Labels],
    fetch_lexicon: Callable[[LexiconSheet], Labels],
    complements: dict[str, Labels],
) -> list[tuple[str, str, str, str]]:
    """Toutes les lignes du fichier de nomenclatures, colonne par colonne."""
    unknown = sorted(set(complements) - set(COLUMN_SOURCES))
    if unknown:
        raise ValueError(f"Compléments pour des colonnes inconnues : {unknown}")
    rows = []
    for code_damir, sources in COLUMN_SOURCES.items():
        loaded = [
            (s.source, fetch_snds(s) if isinstance(s, SndsTable) else fetch_lexicon(s))
            for s in sources
        ]
        rows += merge_column(code_damir, loaded, complements.get(code_damir, {}))
    return rows


def _download(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=60) as response:
        return response.read()


def main() -> None:
    """Télécharge les sources figées et réécrit reference/nomenclatures.csv."""
    lexicon = xlrd.open_workbook(file_contents=_download(LEXICON_URL))
    snds_cache: dict[str, str] = {}

    def fetch_snds(table: SndsTable) -> Labels:
        if table.table not in snds_cache:
            snds_cache[table.table] = _download(SNDS_URL.format(table=table.table)).decode("utf-8")
        return parse_snds_table(snds_cache[table.table], table)

    rows = build_rows(
        fetch_snds, lambda sheet: parse_lexicon_sheet(lexicon, sheet), read_complements()
    )
    with OUTPUT_FILE.open("w", encoding="utf-8", newline="\n") as f:
        writer = csv.writer(f, delimiter=";", lineterminator="\n")
        writer.writerow(OUTPUT_COLUMNS)
        writer.writerows(rows)
    print(f"{len(rows)} libellés pour {len(COLUMN_SOURCES)} colonnes : {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
