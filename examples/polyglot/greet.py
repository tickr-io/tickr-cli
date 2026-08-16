import subprocess


greeting = subprocess.check_output(
    ["tickr-ctx", "get", "greeting", "--signal", "--default", "Hello from Tickr"],
    text=True,
).strip()
print(f"python: {greeting}")
