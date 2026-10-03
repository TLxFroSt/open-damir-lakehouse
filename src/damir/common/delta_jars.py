"""Jars Delta Lake : téléchargés une seule fois, puis donnés à Spark en chemins locaux.

Pourquoi ne pas laisser Spark les télécharger à chaque démarrage (spark.jars.packages) ?
Dans ce cas, en mode local, Spark recopie chaque jar du driver vers l'exécuteur via un
java.nio Pipe. Sous Windows, une lecture de plus de ~32 Ko dans un Pipe peut bloquer
indéfiniment (bug JDK-8304182, non corrigé) : la session restait figée environ une fois
sur cinq. Avec des URI « local: », Spark considère les jars déjà présents sur le disque
et ne passe plus par ce Pipe. Bonus : plus de résolution Maven (ni de journal Ivy) au
démarrage, et une session qui démarre hors ligne une fois les jars en cache.
"""

import importlib.metadata
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pyspark

JARS_CACHE_DIR = Path.home() / ".cache" / "open-damir-lakehouse" / "spark-jars"
SCALA_VERSION = "2.13"


def delta_maven_coordinate() -> str:
    """Coordonnée Maven du jar Delta assorti aux versions installées de pyspark et delta-spark.

    Même règle que delta.configure_spark_with_delta_pip : un artefact par version
    mineure de Spark, par exemple io.delta:delta-spark_4.2_2.13:4.4.0.
    """
    spark_minor = ".".join(pyspark.__version__.split(".")[:2])
    delta_version = importlib.metadata.version("delta-spark")
    return f"io.delta:delta-spark_{spark_minor}_{SCALA_VERSION}:{delta_version}"


def delta_jar_uris(cache_dir: Path = JARS_CACHE_DIR) -> list[str]:
    """URI « local: » des jars Delta et de leurs dépendances, téléchargés au premier appel.

    Un dossier par coordonnée Maven : changer de version de Delta ou de Spark
    télécharge un nouveau jeu de jars sans mélanger avec l'ancien.
    """
    coordinate = delta_maven_coordinate()
    target_dir = cache_dir / coordinate.replace(":", "_")
    jars_dir = target_dir / "jars"
    # Marqueur écrit seulement après un téléchargement réussi : un dossier incomplet
    # (processus interrompu) n'est jamais pris pour un cache valide
    complete_marker = target_dir / "COMPLETE"
    if not complete_marker.is_file():
        shutil.rmtree(target_dir, ignore_errors=True)
        _download_with_spark_submit(coordinate, target_dir)
        complete_marker.write_text(coordinate, encoding="utf-8")
    # file:///C:/... devient local:///C:/... (même forme sous Linux : local:///home/...)
    return ["local:" + jar.as_uri().removeprefix("file:") for jar in sorted(jars_dir.glob("*.jar"))]


def _download_with_spark_submit(coordinate: str, target_dir: Path) -> None:
    """Télécharge les jars avec la résolution Maven de spark-submit, sans créer de session.

    spark-submit résout --packages (Ivy) puis exécute un script Python vide :
    aucun SparkContext, donc aucun exécuteur et aucune copie par Pipe.
    """
    bin_dir = Path(pyspark.__file__).parent / "bin"
    spark_submit = bin_dir / ("spark-submit.cmd" if os.name == "nt" else "spark-submit")
    env = {**os.environ, "PYSPARK_PYTHON": sys.executable}
    with tempfile.TemporaryDirectory() as tmp:
        noop_script = Path(tmp) / "noop.py"
        noop_script.write_text("", encoding="utf-8")
        subprocess.run(
            [
                str(spark_submit),
                "--conf",
                f"spark.jars.ivy={target_dir.as_posix()}",
                "--packages",
                coordinate,
                str(noop_script),
            ],
            check=True,
            env=env,
        )
