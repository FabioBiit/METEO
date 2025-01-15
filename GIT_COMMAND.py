import os
import subprocess

# Cambia directory al repository
os.chdir("C:/Users/kyros/OneDrive/Desktop/METEO")

# Funzione per eseguire i seguenti comandi git:
def git_command(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Succeeded: {result.stdout}")
    else:
        print(f"Failed: {result.stderr}")

try:
    git_command("git pull")
    git_command("git add .")
    git_command("git commit -m 'Aggiornamento repository'")
    git_command("git push")

except Exception as e:
    print(f"Errore durante l'esecuzione: {e}")