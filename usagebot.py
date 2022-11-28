#! /usr/bin/env python3 

"""
Requires external file token_channel.csv to connect to Discord
Syntax of that file should be:

token,channel
<DISCORD_BOT_TOKEN>,<CHANNEL_ID>

Without token & channel info, this code will print SLURM cluster usage info to stdout.
"""


import pandas as pd
from pssh.clients import ParallelSSHClient
from io import StringIO
import json
import subprocess

def compute_power_per_node():
    m = 465
    #m = 3
    hosts = ["gpu-st-p4d-24xlarge-"+str(i) for i in range(1,m)]
    client = ParallelSSHClient(hosts, timeout=10, pool_size=500)

    output = list(client.run_command('bash -c "for i in {1..5} ; do nvidia-smi --query-gpu=index,power.draw  --format=csv,nounits ; echo  ; sleep 1; done"', stop_on_errors=False))



    node_gpu_to_power_usage = {}
    for o in output:
        try:
            c = "\n".join(o.stdout)
        except TypeError:
            continue
        cs = c.split("\n\n")
        df = None
        for cc in cs:
            dd = pd.read_csv(StringIO(cc))
            dd["power"] = pd.to_numeric(dd[" power.draw [W]"], errors="coerce")
            if df is None:
                df = dd
            else:
                df["power"] += dd["power"]
        df["power"] = df["power"]/5
        for i, p in zip(df["index"], df["power"]):
            node_gpu_to_power_usage[o.host+":"+str(i)] = p
    return node_gpu_to_power_usage

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

# looks like gpu:a100:8(IDX:0-7)
def parse_gpu(gpu):
    gpu = gpu[15:-1]
    if gpu == "N/A":
        return []
    gpus = gpu.split(",")
    fgpus = []
    for g in gpus:
        if "-" in g:
            g = list(range(int(g[0]), 1+int(g[2])))
        else:
            g = [int(g)]
        fgpus.extend(g)

    return fgpus

def backtick(msg):
    lines = msg.split("\n")
    final_lines= []
    for i in range(0, len(lines), 30):
        final_lines.append("```\n")
        final_lines.extend(lines[i:(i+30)])
        final_lines.append("```\n\n")
    return "\n".join(final_lines)

def get_msg():

    node_gpu_to_power_usage = compute_power_per_node()

    a = json.loads(subprocess.check_output(['squeue','--json']).decode("utf8"))
    df = pd.DataFrame(a["jobs"])


    sums = []
    gpu_counts = []
    for gpus,nodes,job_resources,user_name in zip(df["gres_detail"], df["nodes"], df['job_resources'], df['user_name']):
       if 'allocated_nodes' in job_resources:
          allocated_nodes = list(job_resources['allocated_nodes'].values())
       else:
          allocated_nodes = []
       fnodes = expand_nodes(nodes)
       power_usage = 0
       gpu_count = 0
       for i, node in enumerate(fnodes):
          allocated_node=allocated_nodes[i]
          gpu = gpus[i] if i < len(gpus) else None
          cpus_used = allocated_node['cpus']
          if gpu is None:
              gpu = list(range(int(cpus_used/6)))
          else:
              gpu = parse_gpu(gpu)
          gpu_count += len(gpu) 
          for g in gpu:
              power_usage += node_gpu_to_power_usage[node+":"+str(g)] if node+":"+str(g) in node_gpu_to_power_usage else 0
       sums.append(power_usage)
       gpu_counts.append(gpu_count)

    df["sum_power_usage"] = sums
    df["gpu_count"] = gpu_counts

    a = json.loads(subprocess.check_output(['sinfo','--json']).decode("utf8"))

    def count_idle_gpus(a):
        used_gpus=len(parse_gpu(a['gres_used']))
        gpus = int(a['gres'][-1])
        idle_gpus = gpus - used_gpus
        idle_cpus=a['idle_cpus']
        cpus = a['cpus']
        usable_gpus = int(idle_cpus / 6)
        return min(idle_gpus, usable_gpus)


    num_idles = sum([count_idle_gpus(a) for a in  a['nodes'] if 'gpu' in a['name'] and 'POWERED_DOWN' not in a['state_flags']])

    preemptible_accounts = [e[0] for e in [l.split("|") for l in subprocess.check_output(['sacctmgr', 'list', '--parsable', 'Account']).decode("utf8").split("\n")] if len(e) >= 3 and e[2] == "root"]

    def group_per_user_name(df):
        s = df.groupby(["account", "user_name"]).sum()
        g = s[["gpu_count","sum_power_usage"]]
        g["average_power_usage"] = g["sum_power_usage"] / g["gpu_count"]
        g["gpu_efficiency"] = g["average_power_usage"] / 405.0 * 100  
        g["gpu_efficiency"] = g["gpu_efficiency"].fillna(0).astype('int')
        g = g[["gpu_count","gpu_efficiency"]]
        g = g.sort_values("gpu_count")
        return str(g)

    df = df[df["partition"] == "gpu"]
    running = df[df["job_state"] == "RUNNING"]
    pending = df[df["job_state"] == "PENDING"]
    preemptible = running[running["account"].isin(preemptible_accounts)]
    non_preemptible = running[~running["account"].isin(preemptible_accounts)]

    pending_count = sum(pending["gpu_count"].values)
    preemptible_count = sum(preemptible["gpu_count"].values)
    non_preemptible_count = sum(non_preemptible["gpu_count"].values)

    group1 = ""
    group1 += "Pending:\n"+group_per_user_name(pending)+"\n\n"
    group1 += "Preemptible:\n"+group_per_user_name(preemptible)+"\n\n"

    group2 = ""
    group2 += "Non-preemptible:\n"+group_per_user_name(non_preemptible)+"\n\n"

    group3 = ""
    group3 += f"Idle: {num_idles} gpus\n"
    group3 += f"Pending count: {pending_count} gpus\n"
    group3 += f"Preemptible count (these jobs will be killed if needed by non preemtible): {preemptible_count} gpus\n"
    group3 += f"Non pre emptible count: {non_preemptible_count} gpus\n"

    msg = backtick(group1)+backtick(group2)+backtick(group3)
    return msg


discord = False

if discord:
    import discord
    from discord.ext import tasks
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
        print(get_msg())
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
else:
    msg = get_msg()
    print(msg)
