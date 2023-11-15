import tractor.api.query as tq
import tqauth
import sys
from datetime import datetime
import re
import json
import time

TIME_NOW = datetime.now()
TIME = TIME_NOW.strftime('%Y-%m-%d %H:%M:%S')

sys.path.extend(['/sw/pipeline/rendering/renderfarm-tools',
                 '/sw/pipeline/rendering/renderfarm-reporting'])
from reporting_tools import reporting_tools as rt
from tractor_utils import job_task_utils as jtu
from sg_utils import sg_utils

tq.setEngineClientParam(user=tqauth.USERNAME, password=tqauth.PASSWORD)


TODAY = datetime.today().strftime('%m%d%Y')
FILE_DIR = "/mnt/w/PubLogs/render/datadog_assetrenderstats_{}.log".format(TODAY)
LOG_FILE = open(FILE_DIR, "a")
COMMENT = "DataDogAssetStats"

def get_persp(cargv):
    """Getting perspective camera name from invocation command.

    Parameters:
        cargv (list): Invocation command.

    Returns:
        persp (str): Name of perspective camera."""

    cargv_format = ' '.join(cargv)
    persp = re.findall('--render_node ([^ -]*)', cargv_format)
    if not persp:
        persp = re.findall('--render-node=(.*?) ', cargv_format)

    return persp[0]


def get_frame_chunks(cargv):
    """Getting frame chunks from the invocation command.

    Parameters:
        cargv (list): Invocation command.

    Returns:
        persp (str): Frame Range."""

    cargv_format = ' '.join(cargv)
    frm_chunk = re.findall('--frange ([^ ]*)', cargv_format)
    if not frm_chunk:
        frm_chunk = re.findall('-t ([^ ]*)', cargv_format)

    return frm_chunk[0].split(',')

def get_nuke_version(cargv):
    """Getting version name from Nuke command.

    Parameters:
        cargv (list): Invocation command.

    Returns:
        version (str): Version number."""

    cargv_format = ' '.join(cargv)
    version = re.findall('--version ([^ -]*)', cargv_format)[0]

    return version

def grab_sg_asset_data(show):

    sg_inst, sg = sg_utils.get_shotgun(show)
    filters = [["project.Project.tank_name", "is", show]]
    sg_data = sg.find('Asset',
                      filters,
                      ['code', 'shots', 'sg_asset_type', 'sg_episode_debut'])

    sg_page_id = sg.find_one('Page',
                             [["project.Project.tank_name", "is", show]],
                             fields=['id'])

    sg.close()

    return (sg_inst, sg_data, sg_page_id)


def get_asset_type(asset_name, sg_data):

    for asset_dict in sg_data[1]:
        if asset_dict['code'] == asset_name:
            asset_type = asset_dict['sg_asset_type']
            return asset_type


def get_dept_stats(dept):

    if dept == "SHD":
        dept_tq = "title like ['Katana Turntable for:' 'katana_beauty']" \
                  "and title like [Shading LGT_Lighting.v]"
    elif dept == "MDL":
        dept_tq = "title like 'Katana Turntable for:' " \
                  "and title like Model"
    done_jobs = tq.jobs("done "
                        "and stoptime > -18h "
                        "and {} "
                        "and projects not like TUP "
                        "and comment not like {}"
                        "".format(dept_tq, COMMENT), limit=0, archive=True)
    shows = []
    for job in done_jobs:
        if job['projects'][0] not in shows:
            shows.append(job['projects'][0])

    show_sg_dict = {}
    for show in shows:
        show_sg_dict[show] = grab_sg_asset_data(show)

    for job in done_jobs:
        show = job['projects'][0]
        asset = jtu.get_asset(job['title'])
        invos = tq.invocations("jid={} and current".format(job['jid']),
                               columns=['Blade.numslots', 'Command.argv',
                                        'Blade.numcpu', 'Task.title'],
                               archive=True)

        tot_wall_time = 0
        tot_core_time = 0
        asset_type = ''
        version = ''

        persp_dict = {}
        for invo in invos:
            core_elapsed = rt.get_core_time(invo)
            if not asset_type:
                asset_type = get_asset_type(asset, show_sg_dict[show])

            # Turntable Renders
            if "Katana Turntable Renderer" in invo['Task.title']:
                persp = get_persp(invo['Command.argv'])
                if persp not in persp_dict.keys():
                    persp_dict[persp] = {'wall_time': [],
                                         'core_time': [],
                                         'rss': [],
                                         'tid_list': [],
                                         'frames': 0}
                persp_dict[persp]['wall_time'].append(invo['elapsedreal'])
                persp_dict[persp]['core_time'].append(core_elapsed)
                persp_dict[persp]['rss'].append(invo['rss'])
                persp_dict[persp]['tid_list'].append(invo['tid'])
                persp_dict[persp]['frames'] += len(get_frame_chunks(invo['Command.argv']))
            elif "Nuke Turntable Renderer" in invo['Task.title']:
                version = get_nuke_version(invo['Command.argv'])

            # LGT Submitter Template
            elif "Katana_Render" in invo['Task.title']:
                version = jtu.get_version(job['title'])
                persp = job['title'].split('_')[-1]
                if persp not in persp_dict.keys():
                    persp_dict[persp] = {'wall_time': [],
                                         'core_time': [],
                                         'rss': [],
                                         'tid_list': [],
                                         'frames': 0}
                persp_dict[persp]['wall_time'].append(invo['elapsedreal'])
                persp_dict[persp]['core_time'].append(core_elapsed)
                persp_dict[persp]['rss'].append(invo['rss'])
                persp_dict[persp]['tid_list'].append(invo['tid'])
                persp_dict[persp]['frames'] += 1

            tot_wall_time += invo['elapsedreal']
            tot_core_time += core_elapsed


        stoptime = jtu.convert_to_datetime(job['stoptime']).strftime('%Y-%m-%d %H:%M:%S')
        dept = jtu.find_dept(job['title'])

        write_persp = False
        for persp in persp_dict:

            frames = persp_dict[persp]['frames']
            tpf = (sum(persp_dict[persp]['core_time']) / frames) / 36

            job_dict = {'jid': "{}".format(job['jid']),
                        'artist': job['owner'],
                        'stoptime': stoptime,
                        'show': job['projects'][0],
                        'frames': frames,
                        'max_rss': round(max(persp_dict[persp]['rss']), 2),
                        'ctpf_stand': round(tpf, 2),
                        'dept': dept,
                        'asset': asset,
                        'asset_type': asset_type,
                        'perspective': persp,
                        'version': version}


            LOG_FILE.write("{}\n".format(json.dumps(job_dict)))
            write_persp = True


        if write_persp:
            # If job was archived, unarchive it so we can write the comment
            if job['deletetime']:
                tq.undelete('jid={}'.format(job['jid']))
                time.sleep(.5)

            new_comm = "{} | {}".format(COMMENT, job['comment'])
            tq.jattr('jid={}'.format(job['jid']), key='comment', value=new_comm)

            # If the job was originally archived, then archive it again.
            # if job['deletetime']:
            #     tq.delete('jid={}'.format(job['jid']))

def main():
    for dept in ['SHD', 'MDL']:
        get_dept_stats(dept)

main()
