import argparse, subprocess, os

def rsync(src:str, dest:str):
    print("rsync " + os.path.basename(src) + ":")
    ret = subprocess.run(["rsync", "-uvz", src, dest], capture_output=True, text=True)
    print(ret.stdout)
    print(ret.stderr)

if __name__ == "__main__":
    """
    wslで動かす
    pipenv run python deploy.py
    """
    parser = argparse.ArgumentParser(description="deploy")
    parser.add_argument("--hostname", help="hostname", default="aws-ec2-4")
    parser.add_argument("--include", "-i", help="include files for rsync", nargs="*", choices=["bot", "model", "config"])
    parser.add_argument("--modelPath", "-m", help="model path", default="model_path")

    args = parser.parse_args()
    hostname:str = args.hostname
    include_files = args.include
    
    if "bot" in include_files:
        rsync("target/x86_64-unknown-linux-gnu/release/bot", f"{hostname}:~/")
    else:
        print("skip bot")
    if "model" in include_files:
        rsync(args.modelPath, f"{hostname}:~/")
    else:
        print("skip model")
    if "config" in include_files:
        rsync("config.bot.yaml", f"{hostname}:~/")
        rsync("config.yaml", f"{hostname}:~/")
        rsync("cron-settings.crontab", f"{hostname}:~/")
    else:
        print("skip config")

    cmds = f"""\
ssh {hostname} -t << EOL
sudo su -
mkdir -p /usr/local/bot
mv /home/ec2-user/* /usr/local/bot/
crontab /usr/local/bot/cron-settings.crontab
EOL
"""
    ret = subprocess.run(cmds, shell=True, capture_output=True, text=True)
    print(ret.stdout)
    print(ret.stderr)

