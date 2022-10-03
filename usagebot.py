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


def get_usage_msg(
    backticks=True   # whether to add backticks for Discord formatting or not
    ):
    "gets a list of cluster usage from squeue and creates a text message from it"
    a = json.loads(subprocess.check_output(['squeue','--json']).decode("utf8"))
    df = pd.DataFrame(a["jobs"])

    # tally up number of idle nodes
    tally_choice = 'rom1504'   
    if tally_choice == 'rom1504': # json method by rom1504
        a = json.loads(subprocess.check_output(['sinfo','--json']).decode("utf8"))
        num_idles = sum([1 for a in  a['nodes'] if a['state'] == 'idle' and 'gpu' in a['name'] and 'POWERED_DOWN' not in a['state_flags']])
    else:   # shell-based method by drscotthawley
        cmd = "sinfo | grep ' idle ' | awk '{print $4}' | head -n 1"
        ps = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        num_idles = ps.communicate()[0].decode("utf-8").rstrip()

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


print(get_usage_msg(backticks=False))  # get and print cluster usage message to stdout


# (try to) Read Discord bot token and channel id from external file
try:
    info_df = pd.read_csv('token_channel.csv')
    token = info_df['token'][0]
    channel_id = info_df['channel'][0]
    print("Discord authorization info found. Sending message to Discord.")
except:
    print("No Discord credentials found. Exiting.")
    sys.exit(1)                   # might as well just stop here


class UsageBotClient(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # an attribute we can access from our task
        self.counter = 0

    async def setup_hook(self) -> None:
        # start the task to run in the background
        self.my_background_task.start()

    async def on_ready(self):
        return

    @tasks.loop(hours=12)  
    async def my_background_task(self):
        channel = self.get_channel(channel_id)  # channel ID goes here
        await channel.send(get_usage_msg())

    @my_background_task.before_loop
    async def before_my_task(self):
        await self.wait_until_ready()  # wait until the bot logs in


# Initialize bot client object
client = UsageBotClient(intents=discord.Intents.default())

# Run the bot
client.run(token)

