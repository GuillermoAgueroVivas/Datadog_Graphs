import tractor.api.query as tq
import tqauth
import time
import sys
from datetime import datetime
import json

TIME_NOW = datetime.now()
TIME = TIME_NOW.strftime('%Y-%m-%d %H:%M:%S')

sys.path.extend(['/sw/pipeline/rendering/renderfarm-tools',
                 '/sw/pipeline/rendering/renderfarm-reporting'])
from reporting_tools import reporting_tools as rt
from tractor_utils import job_task_utils as jtu

tq.setEngineClientParam(user=tqauth.USERNAME, password=tqauth.PASSWORD)

TODAY = datetime.today().strftime('%m%d%Y')
FILE_DIR = "/mnt/w/PubLogs/render/datadog_lgtrenderstats_{}.log".format(TODAY)
LOG_FILE = open(FILE_DIR, "a")
COMMENT = "DataDogLGTStats"

done_jobs = tq.jobs("title like LGT.v and title like .katana_ and "
                    "done and comment not like {} and "
                    "spooltime > -7d and "
                    "stoptime > -18h and "
                    "service like Linux64".format(COMMENT), limit=500)
for job in done_jobs:

    shot = jtu.get_shot(job['title'])
    seq = jtu.get_sequence(job['title'])
    epi = jtu.get_episode(job['title'])
    passname = jtu.get_passname(job['title'])
    dept = jtu.find_dept(job['title'])

    invos = tq.invocations("jid={} and current".format(job['jid']),
                           columns=['Blade.numslots', 'Blade.numcpu',
                                    'Task.title'])
    job_coretime, job_walltime = 0, 0
    frames, ren_coretime, ren_walltime, ren_rss = 0, 0, 0, []
    for invo in invos:
        core_time = rt.get_core_time(invo)

        job_coretime += core_time
        job_walltime += invo['elapsedreal']

        if "Katana_Render" in invo['Task.title']:
            frames += 1
            ren_coretime += core_time
            ren_walltime += invo['elapsedreal']
            ren_rss.append(invo['rss'])

        stoptime = jtu.convert_to_datetime(job['stoptime'])

    job_dict = {'jid': "{}".format(job['jid']),
                'artist': job['owner'],
                'stoptime': stoptime.strftime('%Y-%m-%d %H:%M:%S'),
                'show': job['projects'][0],
                'core_time': int(job_coretime),
                'episode': epi,
                'sequence': seq,
                'shot': shot,
                'passname': passname,
                'dept': dept,
                'state': "done",
                'frames': frames,
                'lgt_type': 'FFR' if frames > 3 else 'FML',
                'max_rss': max(ren_rss),
                'ctpf_stand': int((ren_coretime / 36) / frames)}
    LOG_FILE.write("{}\n".format(json.dumps(job_dict)))

    # If job was archived, unarchive it so we can write the comment
    if job['deletetime']:
        tq.undelete('jid={}'.format(job['jid']))
        time.sleep(.5)

    new_comm = "{} | {}".format(COMMENT, job['comment'])
    tq.jattr('jid={}'.format(job['jid']), key='comment', value=new_comm)

    # If the job was originally archived, then archive it again.
    if job['deletetime']:
        tq.delete('jid={}'.format(job['jid']))
