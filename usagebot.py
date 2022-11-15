#! /usr/bin/env python3 

"""
SLURM usage tracker Discord bot by drscotthawley & rom1504

Requires external file token_channel.csv to connect to Discord
Syntax of that file should be:

token,channel
<DISCORD_BOT_TOKEN>,<CHANNEL_ID>

Without token & channel info, this code will print SLURM cluster usage info to stdout.


initial code by rom1504 at https://gist.github.com/rom1504/09bcafe41605be441dbbbfaaf500f42e
"""

import discord
from discord.ext import tasks
import json
import pandas as pd
import subprocess
import sys
import pandas as pd
from pssh.clients import ParallelSSHClient
from io import StringIO

def compute_power_per_node():
    m = 465
    #m = 3
    hosts = ["gpu-st-p4d-24xlarge-"+str(i) for i in range(1,m)]
    client = ParallelSSHClient(hosts)

    output = list(client.run_command('nvidia-smi --query-gpu="index,serial,power.draw"  --format=csv'))



    node_to_power_usage = {}
    for o in output:
        df = pd.read_csv(StringIO("\n".join(o.stdout)))
        df["power"] = pd.to_numeric(df[' power.draw [W]'].apply(lambda a:a.replace("W", "").replace(" ", "")), errors='coerce')
        m = float(df["power"].mean())
        node_to_power_usage[o.host] = m

    return node_to_power_usage

def expand_nodes(s):
        s = s.replace("gpu-st-p4d-24xlarge-", "").replace("[","").replace("]","")
        ns = list(s.split(","))
        nodes = []
        for n in ns:
            if n  == "":
                continue
            if "-" in n:
                b = int(n.split("-")[0])
                e = int(n.split("-")[1])
                nodes.extend(list(range(b,e+1)))
            else:
                nodes.append(int(n))
        return ["gpu-st-p4d-24xlarge-"+str(n) for n in nodes]

def get_msg(
    backticks=True   # whether to add backticks for Discord formatting or not
    ):
    "gets a list of cluster usage from squeue and creates a text message from it"
    enable_power_computation = False
    if enable_power_computation:
        node_to_power_usage = compute_power_per_node()
    
    a = json.loads(subprocess.check_output(['squeue','--json']).decode("utf8"))
    df = pd.DataFrame(a["jobs"])
    
    if enable_power_computation:
        sums = []
        for nodes in df["nodes"]:
            fnodes = expand_nodes(nodes)
            power_usages = [node_to_power_usage[n] if n in node_to_power_usage else 0 for n in fnodes]
            sum_pw = sum(power_usages)
            sums.append(sum_pw)

        df["sum_power_usage"] = sums

    a = json.loads(subprocess.check_output(['sinfo','--json']).decode("utf8"))
    num_idles = sum([1 for a in  a['nodes'] if a['state'] == 'idle' and 'gpu' in a['name'] and 'POWERED_DOWN' not in a['state_flags']])

    preemptible_accounts = [e[0] for e in [l.split("|") for l in subprocess.check_output(['sacctmgr', 'list', '--parsable', 'Account']).decode("utf8").split("\n")] if len(e) >= 3 and e[2] == "root"]

    def group_per_user_name(df):
        if enable_power_computation:
            cols = ["node_count", "sum_power_usage"]
        else:
            cols = ["node_count"]
        g = df.groupby(["account", "user_name"]).sum()[cols]
        if enable_power_computation:
            g["average_power_usage"] = g["sum_power_usage"] / g["node_count"]
            g["gpu_efficiency"] = g["average_power_usage"] / 405.0 * 100
        g = g.sort_values("node_count")
        g = g.round(0)
        return str(g)

    df = df[df["partition"] == "gpu"]
    running = df[df["job_state"] == "RUNNING"]
    pending = df[df["job_state"] == "PENDING"]
    preemptible = running[running["account"].isin(preemptible_accounts)]
    non_preemptible = running[~running["account"].isin(preemptible_accounts)]

    pending_count = sum(pending["node_count"].values)
    preemptible_count = sum(preemptible["node_count"].values)
    non_preemptible_count = sum(non_preemptible["node_count"].values)

    msg = "```\n" if backticks else ""
    msg += "Pending:\n"+group_per_user_name(pending)+"\n\n"
    msg += "Preemptible:\n"+group_per_user_name(preemptible)+"\n\n"
    msg += "Non-preemptible:\n"+group_per_user_name(non_preemptible)+"\n\n"
    msg += f"Idle: {num_idles} nodes\n"
    msg += f"Pending count: {pending_count} nodes\n"
    msg += f"Preemptible count (these jobs will be killed if needed by non preemtible): {preemptible_count} nodes\n"
    msg += f"Non pre emptible count: {non_preemptible_count} nodes\n"
    msg = msg+"\n```" if backticks else msg
    return msg



# Read Discord bot token and channel id from external file
try:
    info_df = pd.read_csv('token_channel.csv')
    token = info_df['token'][0]
    channel_id = info_df['channel'][0]
    print("Discord authorization info found.")
    print("  token =",token)
    print("  channel_id =",channel_id)
    print("Proceeding.")
except:
    print("No Discord credentials found. Here's a printout:\n")
    print(get_msg(backticks=False))
    sys.exit(1)                   # might as well just stop here


# Initialize bot client object
client = discord.Client()


# Setup background task 
@tasks.loop(hours=12)
async def my_background_task():
    """A background task that gets invoked every __ hours."""
    channel = client.get_channel(channel_id) 
    await channel.send(get_msg())
    
@my_background_task.before_loop
async def my_background_task_before_loop():
    await client.wait_until_ready()

my_background_task.start()


# Run the bot
client.run(token)
