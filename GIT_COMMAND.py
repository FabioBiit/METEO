import subprocess

def run_git_command(command):
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Successo: {result.stdout}")
    else:
        print(f"Errore: {result.stderr}")

run_git_command("git pull")
run_git_command("git add .")
run_git_command('git commit -m "Aggiornamenti automatici"')
run_git_command("git push")
