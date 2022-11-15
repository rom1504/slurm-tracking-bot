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

def get_msg(
    backticks=True   # whether to add backticks for Discord formatting or not
    ):
    "gets a list of cluster usage from squeue and creates a text message from it"
    a = json.loads(subprocess.check_output(['squeue','--json']).decode("utf8"))
    df = pd.DataFrame(a["jobs"])

    a = json.loads(subprocess.check_output(['sinfo','--json']).decode("utf8"))
    num_idles = sum([1 for a in  a['nodes'] if a['state'] == 'idle' and 'gpu' in a['name'] and 'POWERED_DOWN' not in a['state_flags']])

    preemptible_accounts = [e[0] for e in [l.split("|") for l in subprocess.check_output(['sacctmgr', 'list', '--parsable', 'Account']).decode("utf8").split("\n")] if len(e) >= 3 and e[2] == "root"]

    def group_per_user_name(df):
        return str(df.groupby(["account", "user_name"]).sum("node_count")[["node_count"]].sort_values("node_count"))

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
