import subprocess
import re
import sys


if __name__ == "__main__":
    cmds = [s for s in sys.argv[1:] if re.search(r"^\d+$", s)]
    if cmds:
        filename = f"export PORT={cmds[0]}"
    else:
        filename = f"export PORT=8080"
    with open(".env", "w") as f:
        f.write(filename)

    build_cmd = "docker-compose up"
    for cmd in sys.argv[1:]:
        if re.search(r"^\d+$", cmd):
            continue
        build_cmd += f" {cmd}"

    print("build_cmd:", build_cmd)
    build_result = subprocess.call(build_cmd.split())
    print("build_result:", build_result)
