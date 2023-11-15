import argparse
import os
import sys
import time
import tqauth
import json
import re # For frame number in ANM breakdown
from datetime import datetime
import tractor.api.query as tq
sys.path.extend(['/sw/pipeline/rendering/renderfarm-tools',
                 '/sw/pipeline/rendering/renderfarm-reporting'])
from tractor_utils import job_task_utils as jtu
from reporting_tools import reporting_tools as rt
from sg_utils import sg_utils

tq.setEngineClientParam(user=tqauth.USERNAME, password=tqauth.PASSWORD)

# User Input and Globals
PARSER = argparse.ArgumentParser(description="Get LGT TPF per Show/Episode/Shot. "
                                             "Example: get_tpf -show TUP -epi A104 -v")
PARSER.add_argument("-s", "--show", type=str, help="Show to grab the TPF for. "
                                                   "Example: '-show TUP'")

PARSER.add_argument("-e", "--episode", type=str, help="Episode to grab the TPF for. "
                                                      "Example: '-epi A104'")
PARSER.add_argument("-sh", "--shot", type=str, help="Shot to grab the TPF for. "
                                                    "Example: '-shot B103_CE1890'")

PARSER.add_argument('-epi', action='store_true', help="Show Episode Breakdown")
PARSER.add_argument('-seq', action='store_true', help="Show Sequence Breakdown")
PARSER.add_argument('-shot', action='store_true', help="Show Shot Breakdown")
PARSER.add_argument('-seq_shot', action='store_true', help="Seq and Shot Breakdown")
PARSER.add_argument('-all_breakdown', action='store_true', help="Epi, Seq and Shot Breakdown")

PARSER.add_argument('-all_epi', action='store_true', help="Grab stats for all Episodes")
PARSER.add_argument('-daily_epi', action='store_true', help="Grab stats for Episodes that rendered in last 24 hours")

ARGS = PARSER.parse_args()
SHOW = ARGS.show.upper()
EPISODE = ARGS.episode
SHOT = ARGS.shot

if ARGS.epi:
    BREAKDOWNS = ['epi']
elif ARGS.seq:
    BREAKDOWNS = ['seq']
elif ARGS.shot:
    BREAKDOWNS = ['shot']
elif ARGS.seq_shot:
    BREAKDOWNS = ['seq', 'shot']
elif ARGS.all_breakdown:
    BREAKDOWNS = ['epi', 'seq', 'shot']

DAILY_EPISODES = bool(ARGS.daily_epi)
ALL_EPISODES = bool(ARGS.all_epi)

TODAY = datetime.today().strftime('%m%d%Y')
TIME_NOW = datetime.now()
TIME = TIME_NOW.strftime('%Y-%m-%d %H:%M:%S')
FILE_DIR = "/mnt/w/PubLogs/render/datadog_epi_seq_stats_{}.log".format(TODAY)
# FILE_DIR = "datadog_epi_seq_stats_{}.log".format(TODAY)

with open('/sw/tractor/reporting_data/cg_keyshots.json') as json_file:
    KEYSHOTS = json.load(json_file)
with open('/sw/tractor/reporting_data/cg_parentshots.json') as json_file:
    PARENTSHOTS = json.load(json_file)

IGNORE_EPI = []
if SHOW == "LMA":
    IGNORE_EPI = ['R001']
elif SHOW == "MLP":
    IGNORE_EPI = ['C001', 'E999', 'M001', 'M002', 'T001', 'T002']
elif SHOW == "PWP":
    IGNORE_EPI = ['C101', 'M101', 'N101', '0101', 'O101', 'R101', 'R102',
                  'S101', 'S102', 'S103', 'S104', 'S105', 'S106', 'S107',
                  'S108', 'S109', 'S110', 'S111', 'S112', 'S113', 'S114',
                  'S115', 'S116', 'S117', 'S118', 'T101', 'T102']
elif SHOW == "TUP":
    IGNORE_EPI = ['E800', 'E801', 'E901', 'E991', 'E993', 'E995', 'M002',
                  'M003', 'S001', 'S002', 'S003', 'S004', 'S005', 'S006',
                  'S007', 'S008', 'S009', 'T001', 'T006', 'T007']
elif SHOW == "ZOM":
    IGNORE_EPI = ['P001', 'R001', 'R002', 'R003', 'R004', 'R005']

# Setting the shows season
if SHOW == "TUP":
    SEASON = "TUP002"
elif SHOW == "LMA":
    SEASON = "SST003"
else:
    SEASON = "{}001".format(SHOW)


def get_episodes_rendered():
    """Get the episode names that have rendered for the show"""

    tq_addition = ""
    # If we just want to grab the episodes rendered in the last 24 hours
    if DAILY_EPISODES:
        tq_addition = "and stoptime > -24h"

    # 24 hour episode search
    jobs_rendered = tq.jobs("done and projects like {0} and "
                            "title like 'QC Render' and "
                            "title like {1} {2}"
                            "".format(SHOW, SEASON, tq_addition),
                            limit=0,
                            archive=True)

    # We just want the unique epiosde names.
    episodes_rendered = set()
    for job in jobs_rendered:
        episode = jtu.get_episode(job['title'])
        episodes_rendered.add(episode)

    return list(episodes_rendered)


def datadog_print(breakdown, stats_dict, prod_order):

    def create_datadog_dict(sdict):

        out_dict = {
            'timestamp': TIME,
            'show': SHOW,
            'anm_shots': sdict['shots'],
            'anm_mrss': round(sdict['anm_mrss'], 2),
            'anm_jobs': round(sdict['anm_jobs'], 2),
            'anm_chps': round((sdict['anm_chps'] / 3600.0), 2),
            'anm_tpf': int(sdict['anm_tpf'] / 36),
            'anm_frames': sdict['anm_frames'],
            'num_assets': round(sdict['num_assets'], 2)
            }
        return out_dict


    log_file = open(FILE_DIR, "a")
    for epi in stats_dict:
        p_order = prod_order.get(epi, 99)

        if breakdown == 'epi':
            if epi in IGNORE_EPI:
                continue
            elif stats_dict[epi]['shots'] < 3:
                continue
            out_dict = create_datadog_dict(stats_dict[epi])
            out_dict['breakdown_type'] = 'episode'
            out_dict['episode'] = epi
            out_dict['prod_order'] = p_order
            log_file.write("{}\n".format(json.dumps(out_dict)))

        elif breakdown == 'seq':
            if epi in IGNORE_EPI:
                continue
            for seq in stats_dict[epi]['seq_list']:
                out_dict = create_datadog_dict(stats_dict[epi][seq])
                out_dict['breakdown_type'] = 'sequence'
                out_dict['episode'] = epi
                out_dict['sequence'] = seq
                out_dict['prod_order'] = p_order
                log_file.write("{}\n".format(json.dumps(out_dict)))

        elif breakdown == 'shot':
            for seq in stats_dict[epi]['seq_list']:
                for shot in stats_dict[epi][seq]['shot_list']:
                    stats_dict[epi][seq][shot]['shots'] = 1
                    out_dict = create_datadog_dict(stats_dict[epi][seq][shot])
                    out_dict['breakdown_type'] = 'shot'
                    out_dict['episode'] = epi
                    out_dict['sequence'] = seq
                    out_dict['shot'] = shot
                    out_dict['prod_order'] = p_order
                    out_dict['shot_tag'] = '-'
                    if shot in KEYSHOTS[SHOW]:
                        out_dict['shot_tag'] = 'Key'
                    elif shot in PARENTSHOTS[SHOW]:
                        out_dict['shot_tag'] = 'Parent'
                    log_file.write("{}\n".format(json.dumps(out_dict)))

def get_shot_assets(sg):

    filters = [["project.Project.tank_name", "is", SHOW]]

    all_shots = sg.find("Shot", filters, fields=['assets', 'code'],
                        order=[{"direction": "asc", "field_name": "id"}])
    all_assets = sg.find("Asset", filters, fields=['code', 'sg_asset_type'],
                         order=[{"direction": "asc", "field_name": "id"}])

    all_assets_dict = {asset['id']: asset for asset in all_assets}

    shot_asset_dict = {}
    for shot in all_shots:
        shot_asset_dict[shot['code']] = {'num_assets': 0}
        sass_dict = shot_asset_dict[shot['code']]
        shot['assets'] = [all_assets_dict[asset['id']] for asset in shot['assets']]
        for asset in shot['assets']:
            sass_dict['num_assets'] += 1
            if asset['sg_asset_type'] in sass_dict.keys():
                sass_dict[asset['sg_asset_type']] += 1
            else:
                sass_dict[asset['sg_asset_type']] = 1

    return shot_asset_dict

def get_production_order(sg):

    filters = [["project.Project.tank_name", "is", SHOW]]
    fields = ['code', 'sg_production_order']

    sg_epi_dict = sg.find("Episode", filters, fields)

    prod_epi_order = {}
    for epi_dict in sg_epi_dict:
        episode = epi_dict['code'][-4:]
        prod_order = epi_dict['sg_production_order']
        if not prod_order:
            prod_order = 99

        prod_epi_order[episode] = prod_order

    return prod_epi_order


def add_comment(new_jobs):

    num_jobs = len(new_jobs)
    i = 0
    for job in new_jobs:
        # time.sleep(.5)
        i += 1
         # If job was archived, unarchive it so we can write the comment
        if job['deletetime']:
            tq.undelete('jid={}'.format(job['jid']))
            time.sleep(1)

        new_comm = "EpiShotStats | {}".format(job['comment'])
        tq.jattr('jid={}'.format(job['jid']), key='comment', value=new_comm)


def get_episode_data(epi, dept):

    epi_name = epi.split("_")[0]
    epi_stats_log = ("/sw/pipeline/rendering/renderfarm-reporting"
                     "/tq_data/epi_shot_stats/"
                     "{0}/{1}/{2}_{3}_{1}_stats.log"
                     "").format(SHOW, dept, SEASON, epi_name)
    # Stats Directory
    stats_dir = epi_stats_log.replace(epi_stats_log.split('/')[-1], '')

    # Read Old Data
    if os.path.exists(epi_stats_log):
        with open(epi_stats_log, 'r') as f_obj:
            epi_data = json.loads(f_obj.read())
    else:
        if not os.path.isdir(stats_dir):
            os.makedirs(stats_dir)
        epi_data = []

    # Find New Data from Tractor
    if dept == "LGT":
        job_tq = ("Job.title like LGT.v and Job.title like katana "
                  "and Job.title not like Groom_QC")
        task_tq = "Task.title like Katana_Render"

    elif dept == "ANM":
        job_tq = "Job.title like 'QC Render'"
        task_tq = "Task.title like 'Render Katana'"

    tq_search = ("projects like {} and "
                 "Job.comment not like EpiShotStats and "
                 "Job.title like _{} and "
                 "Job.numdone = Job.numtasks and "
                 "{} and {} and current and "
                 "rcode = 0".format(SHOW, epi, job_tq, task_tq))

    tq_columns = ['Job.title', 'jid', 'elapsedreal',
                  'Blade.numcpu', 'Blade.profile',
                  'Blade.name', 'Blade.numslots',
                  'rss', 'Job.owner', 'Task.title']

    invo_data = tq.invocations(tq_search,
                               columns=tq_columns,
                               sortby=['jid', 'tid'],
                               limit=0,
                               archive=True)

    # Grab all the jids in the old and new data
    old_jids = list(set(invo['jid'] for invo in epi_data))
    new_jids = list(set(invo['jid'] for invo in invo_data))

    # If new jids are in the old jids we save them to a list
    duplicate_jids = set()
    for jid in new_jids:
        if jid in old_jids:
            duplicate_jids.add(jid)
    duplicate_jids_list = list(duplicate_jids)

    # We will then add the EpiShotsStats comment to those jobs as
    # they should of already had that commment added
    if duplicate_jids_list:
        jids_query = ' '.join(str(e) for e in duplicate_jids_list)
        fix_jobs = tq.jobs("jid in [{}] and comment not like EpiShotStats"
                           "".format(jids_query),
                           columns=['deletetime', 'comment', 'jid'],
                           limit=0, archive=True)
        add_comment(fix_jobs)
        # Force script to error so we don't add duplicate data.
        sys.exit("Please Retry.\nDuplicate Jids: {}"
                 "".format(duplicate_jids_list))

    # Merging old data and new data into a new list
    all_data = epi_data + invo_data
    if invo_data:

        # Write the merged data to the file.
        with open(epi_stats_log, 'w') as f_obj:
            f_obj.write(json.dumps(all_data))

        # Now that we've wrote the new data, we need to add a comment
        # to those josb so we don't add them again.
        new_jids = list(set(invo['jid'] for invo in invo_data))
        jids_query = ' '.join(str(e) for e in new_jids)
        new_jobs = tq.jobs("jid in [{}]".format(jids_query),
                           columns=['deletetime', 'comment', 'jid'],
                           limit=0, archive=True)
        add_comment(new_jobs)

        # Double Check the jobs, if comment isn't there, try add it again.
        check_jobs = tq.jobs("jid in [{}] and comment not like EpiShotStats"
                             "".format(jids_query),
                             columns=['deletetime', 'comment', 'jid'],
                             limit=0, archive=True)
        add_comment(check_jobs)

    return all_data


def get_num_frames(ttitle): #ADD TO RTOOLS
    """
    Get Number of frames from a task title with 1001-1010 format.
    """

    if re.findall('[0-9]+-[0-9]+', ttitle):
        ftitle = re.findall('[0-9]+-[0-9]+', ttitle)[0]
        firstframe = int(ftitle.split('-')[0])
        lastframe = int(ftitle.split('-')[1])
        framecount = lastframe - firstframe + 1
    elif re.findall('[0-9]+', ttitle):
        framecount = 1
    else:
        framecount = 1

    return framecount

def anm_breakdown(dept_data):

    anm_dict = {'seq_list': set()}
    for invo in dept_data:
        shot = jtu.get_shot(invo['Job.title'])
        seq = jtu.get_sequence(invo['Job.title'])

        # Setting up ANM dictionary
        if seq not in anm_dict.keys():
            anm_dict[seq] = {'shot_list': set()}
        if shot not in anm_dict[seq].keys():
            anm_dict[seq][shot] = {'job_dict': {}, 'anm_jobs': 0,
                                   'anm_chps': [], 'anm_frames': 0,
                                   'anm_tpf': [], 'anm_mrss': 0}
        anm_dict['seq_list'].add(seq)
        anm_dict[seq]['shot_list'].add(shot)

        ctime = rt.get_core_time(invo)
        frames = get_num_frames(invo['Task.title'])

        # Adding up the core time per task
        if invo['jid'] not in anm_dict[seq][shot]['job_dict'].keys():
            anm_dict[seq][shot]['job_dict'][invo['jid']] = {
                'core_runtime': ctime, 'frames': frames,
                'anm_memory': [invo['rss']],}
        else:
            anm_dict[seq][shot]['job_dict'][invo['jid']]['core_runtime'] += ctime
            anm_dict[seq][shot]['job_dict'][invo['jid']]['frames'] += frames
            anm_dict[seq][shot]['job_dict'][invo['jid']]['anm_memory'].append(invo['rss'])

    for seq in anm_dict['seq_list']:
        for shot in anm_dict[seq]['shot_list']:
            for job in anm_dict[seq][shot]['job_dict']:
                # Counting number of ANM jobs
                anm_dict[seq][shot]['anm_jobs'] += 1
                jdict = anm_dict[seq][shot]['job_dict'][job]
                anm_dict[seq][shot]['anm_chps'].append(jdict['core_runtime'])
                # ANM Frame count in shot
                if anm_dict[seq][shot]['anm_frames'] < jdict['frames']:
                    anm_dict[seq][shot]['anm_frames'] = jdict['frames']
                # Adding TPF of job to TPF shot list
                anm_dict[seq][shot]['anm_tpf'].append(
                    jdict['core_runtime'] / jdict['frames'])

            # Getting the average Core Hours Per ANM Shot
            if anm_dict[seq][shot]['anm_chps']:
                anm_dict[seq][shot]['anm_chps'] = (
                    sum(anm_dict[seq][shot]['anm_chps']) /
                    len(anm_dict[seq][shot]['anm_chps']))
            else:
                anm_dict[seq][shot]['anm_chps'] = 0
            # Getting the average TPF Per ANM Shot
            if anm_dict[seq][shot]['anm_tpf']:
                anm_dict[seq][shot]['anm_tpf'] = (
                    sum(anm_dict[seq][shot]['anm_tpf']) /
                    len(anm_dict[seq][shot]['anm_tpf']))
            else:
                anm_dict[seq][shot]['anm_tpf'] = 0

    return anm_dict


def get_shot_stats(seq_dict, seq, shot, shot_asset_dict):

    for job in seq_dict[seq][shot]['job_dict']:
        jdict = seq_dict[seq][shot]['job_dict'][job]
        # Core time per frame and Max RSS memory per job.
        jdict['anm_mrss'] = max(jdict['anm_memory'])

        # Max Memory of shot
        if jdict['anm_mrss'] > seq_dict[seq][shot]['anm_mrss']:
            seq_dict[seq][shot]['anm_mrss'] = jdict['anm_mrss']

    # Number of assets in the shot
    seq_dict[seq][shot]['num_assets'] = shot_asset_dict[shot]['num_assets']

    return seq_dict

def get_seq_stats(seq_dict, seq):

    seq_dict[seq]['shots'] = 0
    seq_dict[seq]['anm_mrss'] = []
    seq_dict[seq]['anm_jobs'] = []
    seq_dict[seq]['anm_chps'] = []
    seq_dict[seq]['anm_tpf'] = []
    seq_dict[seq]['anm_frames'] = 0
    seq_dict[seq]['num_assets'] = []

    # Looping through each shot in the seqeunce to get shot avg.
    for shot in seq_dict[seq]['shot_list']:
        sdict = seq_dict[seq][shot]
        seq_dict[seq]['shots'] += 1
        seq_dict[seq]['anm_mrss'].append(sdict['anm_mrss'])
        seq_dict[seq]['anm_jobs'].append(sdict['anm_jobs'])
        seq_dict[seq]['anm_chps'].append(sdict['anm_chps'])
        seq_dict[seq]['anm_tpf'].append(sdict['anm_tpf'])
        seq_dict[seq]['anm_frames'] += sdict['anm_frames']
        seq_dict[seq]['num_assets'].append(sdict['num_assets'])

    # Seq Avg Max RSS per shot
    seq_dict[seq]['anm_mrss'] = (sum(seq_dict[seq]['anm_mrss']) /
                                 float(len(seq_dict[seq]['anm_mrss'])))
    # Seq Avg ANM Jobs per shot
    seq_dict[seq]['anm_jobs'] = (sum(seq_dict[seq]['anm_jobs']) /
                                 float(len(seq_dict[seq]['anm_jobs'])))
    # Seq Avg ANM Core Hours per shot
    seq_dict[seq]['anm_chps'] = (sum(seq_dict[seq]['anm_chps']) /
                                 float(len(seq_dict[seq]['anm_chps'])))
    # Seq Avg ANM TPF per shot
    seq_dict[seq]['anm_tpf'] = (sum(seq_dict[seq]['anm_tpf']) /
                                float(len(seq_dict[seq]['anm_tpf'])))
    # Seq Avg Number of Assets per shot
    seq_dict[seq]['num_assets'] = (sum(seq_dict[seq]['num_assets']) /
                                   float(len(seq_dict[seq]['num_assets'])))

    return seq_dict

def get_epi_stats(seq_dict):

    seq_dict['shots'] = 0
    seq_dict['anm_mrss'] = []
    seq_dict['anm_jobs'] = []
    seq_dict['anm_chps'] = []
    seq_dict['anm_tpf'] = []
    seq_dict['anm_frames'] = 0
    seq_dict['num_assets'] = []

    # Looping through each shot in the episode to get shot avg.
    for seq in seq_dict['seq_list']:
        for shot in seq_dict[seq]['shot_list']:
            sdict = seq_dict[seq][shot]
            seq_dict['shots'] += 1
            seq_dict['anm_mrss'].append(sdict['anm_mrss'])
            seq_dict['anm_jobs'].append(sdict['anm_jobs'])
            seq_dict['anm_chps'].append(sdict['anm_chps'])
            seq_dict['anm_tpf'].append(sdict['anm_tpf'])
            seq_dict['anm_frames'] += sdict['anm_frames']
            seq_dict['num_assets'].append(sdict['num_assets'])

    # Epi Avg Max RSS per shot
    seq_dict['anm_mrss'] = (sum(seq_dict['anm_mrss']) /
                            float(len(seq_dict['anm_mrss'])))
    # Epi Avg ANM Jobs per shot
    seq_dict['anm_jobs'] = (sum(seq_dict['anm_jobs']) /
                            float(len(seq_dict['anm_jobs'])))
    # Epi Avg ANM Core Hours per shot
    seq_dict['anm_chps'] = (sum(seq_dict['anm_chps']) /
                            float(len(seq_dict['anm_chps'])))
    # Epi Avg ANM Core Hours per shot
    seq_dict['anm_tpf'] = (sum(seq_dict['anm_tpf']) /
                           float(len(seq_dict['anm_tpf'])))
    # Epi Avg Number of Assets per shot
    seq_dict['num_assets'] = (sum(seq_dict['num_assets']) /
                              float(len(seq_dict['num_assets'])))

    return seq_dict


def build_stats(anm_data, shot_asset_dict):

    seq_dict = anm_breakdown(anm_data)

    for seq in seq_dict['seq_list']:
        # Creating shot stats
        for shot in seq_dict[seq]['shot_list']:
            seq_dict = get_shot_stats(seq_dict, seq, shot, shot_asset_dict)
        # Creating Sequence Stats
        seq_dict = get_seq_stats(seq_dict, seq)
    # Creating Episode Stats
    epi_dict = get_epi_stats(seq_dict)

    return epi_dict


def __main__():

    # Connect to the SG API.
    sg_connection = sg_utils.get_shotgun(SHOW)[1]
    # Get the production order of the episodes on a show
    prod_order = get_production_order(sg_connection)
    # Get the assets that are assigned to a shot.
    shot_asset_dict = get_shot_assets(sg_connection)

    input_epis = [""]
    if DAILY_EPISODES or ALL_EPISODES:
        input_epis = get_episodes_rendered()
    else:
        if EPISODE:
            input_epis = [str(s) for s in EPISODE.split(',')]
    # Sorting the episodes
    input_epis.sort()

    stats_dict = {}
    for episode in input_epis:
        anm_data = get_episode_data(episode, "ANM")


        # Putting stats together
        stats_dict[episode] = build_stats(anm_data, shot_asset_dict)


    # Writing Stats
    for breakdown in BREAKDOWNS:
        datadog_print(breakdown, stats_dict, prod_order)



__main__()
