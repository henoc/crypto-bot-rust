import argparse, subprocess, os, json
from datetime import datetime

def rsync(src:str, dest:str):
    print("rsync " + os.path.basename(src) + ":")
    ret = subprocess.run(["rsync", "-uvz", src, dest], capture_output=True, text=True)
    print(ret.stdout)
    print(ret.stderr)

ARCH_TO_TARGET = {
    "x86_64": "x86_64-unknown-linux-gnu",
    "arm64": "aarch64-unknown-linux-gnu"
}

if __name__ == "__main__":
    """
    wslで動かす
    pipenv run python deploy.py
    """
    parser = argparse.ArgumentParser(description="deploy")
    parser.add_argument("--hostname", "-H", help="hostname", default="bot4")
    parser.add_argument("--include", "-i", help="include files for rsync", nargs="*", choices=["bot", "model", "config"])
    parser.add_argument("--modelPath", "-m", help="model path", default="model.zst")
    parser.add_argument("--startStopInstance", "-s", help="run instance during deploy", action="store_true")

    args = parser.parse_args()
    hostname:str = args.hostname
    include_files = args.include

    print("hostname: " + hostname)
    ret = subprocess.run("aws ec2 describe-instances --filter 'Name=tag:Name,Values=" + hostname + "' --query 'Reservations[].Instances[].{i:InstanceId,a:Architecture}'", shell=True, capture_output=True, text=True)
    print(ret.stderr)
    obj = json.loads(ret.stdout)[0]
    instance_id = obj["i"]
    target_triple = ARCH_TO_TARGET[obj["a"]]

    if args.startStopInstance:
        print(f"start instance. instance_id: {instance_id}")
        unixtime_sec = datetime.now().timestamp()
        ret = subprocess.run(f"aws ec2 start-instances --instance-ids {instance_id} && aws ec2 wait instance-running --instance-ids {instance_id}", shell=True, capture_output=True, text=True)
        print(ret.stdout + "\n" + ret.stderr)
        print(f"start instance done. {datetime.now().timestamp() - unixtime_sec} sec")
    
    if "bot" in include_files:
        print(f"target_triple: {target_triple}")
        rsync(f"target/{target_triple}/release/bot", f"{hostname}:~/")
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

    if args.startStopInstance:
        print("stop instance")
        ret = subprocess.run(f"aws ec2 stop-instances --instance-ids {instance_id} && aws ec2 wait instance-stopped --instance-ids {instance_id}", shell=True, capture_output=True, text=True)
        print(ret.stdout + "\n" + ret.stderr)

