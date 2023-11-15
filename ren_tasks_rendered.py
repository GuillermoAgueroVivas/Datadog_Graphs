import tractor.api.query as tq
import tqauth
import json
from datetime import datetime
import sys

sys.path.extend(['/sw/pipeline/rendering/renderfarm-tools',
                 '/sw/pipeline/rendering/renderfarm-reporting'])
from tractor_utils import job_task_utils as jtu

tq.setEngineClientParam(user=tqauth.USERNAME, password=tqauth.PASSWORD)

TODAY = datetime.today().strftime('%m%d%Y')
TIME_NOW = datetime.now()
TIME = TIME_NOW.strftime('%Y-%m-%d %H:%M:%S')
FILE_DIR = "/mnt/w/PubLogs/render/datadog_rentaskdone_{}.log".format(TODAY)

def pull_data_and_sort():

    ren_tasks = tq.tasks("Job.title like _LGT.v and "
                         "Task.title like Katana_Render and "
                         "done and stoptime > -1h",
                         columns=['Job.title', 'title', 'jid', 'Job.projects'],
                         limit=0, archive=True)

    show_dict = {}
    for task in ren_tasks:
        if task['Job.projects']:
            show = task['Job.projects'][0]
            if show not in show_dict.keys():
                show_dict[show] = {'shot': {}, 'seq': {}, 'epi': {},
                                   'ren_done': 0}
            shot = jtu.get_shot(task['Job.title'])
            seq = jtu.get_sequence(task['Job.title'])
            epi = jtu.get_episode(task['Job.title'])

            if epi not in show_dict[show]['epi'].keys():
                show_dict[show]['epi'][epi] = {'ren_done': 0}
            if seq not in show_dict[show]['seq'].keys():
                show_dict[show]['seq'][seq] = {'ren_done': 0, 'epi': epi}
            if shot not in show_dict[show]['shot'].keys():
                show_dict[show]['shot'][shot] = {'ren_done': 0, 'epi': epi,
                                                 'seq': seq}

            show_dict[show]['ren_done'] += 1
            show_dict[show]['epi'][epi]['ren_done'] += 1
            show_dict[show]['seq'][seq]['ren_done'] += 1
            show_dict[show]['shot'][shot]['ren_done'] += 1
    return show_dict


def main():

    show_dict = pull_data_and_sort()

    farm = 'linux'

    log_file = open(FILE_DIR, "a")

    for show in show_dict:
        show_tot = {'metric_type': 'show_ren_tasks',
                    'timestamp': TIME,
                    'farm': farm,
                    'show': show,
                    'done': show_dict[show]['ren_done']}
        log_file.write("{}\n".format(json.dumps(show_tot)))
        for epi in show_dict[show]['epi']:
            show_epi = {'metric_type': 'episode_ren_tasks',
                        'timestamp': TIME,
                        'farm': farm,
                        'show': show,
                        'episode': epi,
                        'done': show_dict[show]['epi'][epi]['ren_done']}
            log_file.write("{}\n".format(json.dumps(show_epi)))
        for seq in show_dict[show]['seq']:
            show_seq = {'metric_type': 'sequence_ren_tasks',
                        'timestamp': TIME,
                        'farm': farm,
                        'show': show,
                        'episode': show_dict[show]['seq'][seq]['epi'],
                        'sequence': seq,
                        'done': show_dict[show]['seq'][seq]['ren_done']}
            log_file.write("{}\n".format(json.dumps(show_seq)))
        for shot in show_dict[show]['shot']:
            show_shot = {'metric_type': 'shot_ren_tasks',
                         'timestamp': TIME,
                         'farm': farm,
                         'show': show,
                         'episode': show_dict[show]['shot'][shot]['epi'],
                         'sequence': show_dict[show]['shot'][shot]['seq'],
                         'shot': shot,
                         'done': show_dict[show]['shot'][shot]['ren_done']}
            log_file.write("{}\n".format(json.dumps(show_shot)))



main()
