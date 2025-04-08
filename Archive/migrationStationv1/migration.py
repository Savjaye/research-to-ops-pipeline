import subprocess
from pathlib import Path

def executeQueryLocaly(queryPath, scriptPath, outPath=None):

    queries_dir = Path(queryPath)
    script_path = scriptPath

    if outPath:
        out_dir = outPath

    query_files = list(queries_dir.rglob("*.sql"))

    for query in query_files:
        
        if outPath:
            cmd = [str(script_path), out_dir]
        else:
                cmd = [str(script_path)
        subprocess.run(cmd, check=True)

executeQueryLocaly("./querySripts/viewSalutations.sql", "operationResearchMigration copy 2.sh",  c"python.csv")