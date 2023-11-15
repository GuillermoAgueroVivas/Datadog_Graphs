import json
from datetime import datetime, timedelta
import re
import sys
import tqauth
import tractor.api.query as tq

sys.path.insert(0, '/sw/pipeline/rendering/renderfarm-tools')
from tractor_utils import tractor_utils as tu

# tq.setEngineClientParam(user=tqauth.USERNAME, password=tqauth.PASSWORD)

TODAY = datetime.today().strftime('%m%d%Y')
TIME_NOW = datetime.now()
TIME = TIME_NOW.strftime('%Y-%m-%d %H:%M:%S')
FILE_DIR = "/mnt/w/PubLogs/render/datadog_bladestats_{}.log".format(TODAY)
LOG_FILE = open(FILE_DIR, "a")


def get_blade_status(site):
    """Gather Blades that are up and save them to a json file"""

    blades = tq.blades("up", columns=['name', 'profile', 'nimby', 'status',
                                      'numcpu', 'loadavg', 'numslots',
                                      'slotsinuse'],
                       sortby=['profile'], limit=0)

    lin_active, lin_idle, win_active, win_idle = 0, 0, 0, 0
    lin_aws, lin_aws_active, lin_aws_idle = 0, 0, 0
    win_aws, win_aws_active, win_aws_idle = 0, 0, 0
    linux_blades, windows_blades = 0, 0
    wrk_active_blades = 0
    linux_core, windows_core, wrk_core = 0, 0, 0

    nimby, wrkstn, others = 0, 0, 0
    blade_dict = {'linux_farm': {}, 'windows_farm': {}}
    for blade in blades:
        bprofile = blade['profile']
        bcore = blade['numcpu']
        bslots = blade['numslots']

        #NIMBY
        if blade['nimby'] != '':
            nimby += bslots

        # WORKSTATIONS
        elif 'Workstation' in bprofile or 'Linux_nimby' in bprofile:
            if bprofile not in blade_dict['linux_farm'].keys():
                blade_dict['linux_farm'][bprofile] = {'total': 1,
                                                      'active': 0,
                                                      'idle': 0,
                                                      'core': 0}
            else:
                blade_dict['linux_farm'][bprofile]['total'] += 1

            wrk_blade_cores = (bcore / 2)
            wrkstn += bslots
            wrk_core += wrk_blade_cores
            blade_dict['linux_farm'][bprofile]['core'] += wrk_blade_cores

            # Add Active Workstations to Linux Blade Count
            if blade['BladeUse.slotsinuse'] >= 1:
                wrk_active_blades += 1
                blade_dict['linux_farm'][bprofile]['active'] += 1
            else:
                blade_dict['linux_farm'][bprofile]['idle'] += 1

        elif "AWS" in bprofile:
            aws_active, aws_idle = 0, 0

            if blade['BladeUse.slotsinuse'] >= 1:
                aws_active = bslots
            else:
                aws_idle = bslots

            aws_core = (bcore / 2)

            if "Linux" in bprofile:
                aws_farm = 'linux_farm'
                lin_aws += bslots
                lin_aws_active += aws_active
                lin_aws_idle += aws_idle

            elif "Windows" in bprofile:
                aws_farm = 'windows_farm'
                win_aws += bslots
                win_aws_active += aws_active
                win_aws_idle += aws_idle

            if bprofile not in blade_dict[aws_farm].keys():
                blade_dict[aws_farm][bprofile] = {'total': bslots,
                                                  'active': aws_active,
                                                  'idle': aws_idle,
                                                  'core': aws_core}
            else:
                blade_dict[aws_farm][bprofile]['total'] += bslots
                blade_dict[aws_farm][bprofile]['active'] += aws_active
                blade_dict[aws_farm][bprofile]['idle'] += aws_idle
                blade_dict[aws_farm][bprofile]['core'] += aws_core


        # LINUX
        elif 'Linux' in bprofile:
            # Adding different profiles to dict
            if bprofile not in blade_dict['linux_farm'].keys():
                blade_dict['linux_farm'][bprofile] = {'total': bslots,
                                                      'active': 0,
                                                      'idle': 0,
                                                      'core': 0}
            else:
                blade_dict['linux_farm'][bprofile]['total'] += bslots

            # Adding up all the slots and cores
            linux_blades += bslots
            # These two conditions are not hyperthread.
            if bcore == 128 or bcore <= 12:
                linux_blade_core = bcore
            else:
                linux_blade_core = (bcore / 2)
            linux_core += linux_blade_core
            blade_dict['linux_farm'][bprofile]['core'] += linux_blade_core

            # Active
            if blade['BladeUse.slotsinuse'] >= 1:
                lin_active += bslots
                blade_dict['linux_farm'][bprofile]['active'] += bslots
            else:
                lin_idle += bslots
                blade_dict['linux_farm'][bprofile]['idle'] += bslots


        # WINDOWS
        elif 'Windows' in bprofile:
            # Adding different profiles to dict
            if bprofile not in blade_dict['windows_farm'].keys():
                blade_dict['windows_farm'][bprofile] = {'total': bslots,
                                                        'active': 0,
                                                        'idle': 0,
                                                        'core': 0}
            else:
                blade_dict['windows_farm'][bprofile]['total'] += bslots

            windows_blades += bslots
            windoes_blade_core = (bcore / 2)
            windows_core += windoes_blade_core
            blade_dict['windows_farm'][bprofile]['core'] += windoes_blade_core

            # Active
            if blade['BladeUse.slotsinuse'] >= 1:
                win_active += bslots
                blade_dict['windows_farm'][bprofile]['active'] += bslots
            else:
                win_idle += bslots
                blade_dict['windows_farm'][bprofile]['idle'] += bslots

        elif 'Desktop' in bprofile:
            # Adding different profiles to dict
            if bprofile not in blade_dict['windows_farm'].keys():
                blade_dict['windows_farm'][bprofile] = {'total': 1,
                                                        'active': 0,
                                                        'idle': 0,
                                                        'core': 0}
            else:
                blade_dict['windows_farm'][bprofile]['total'] += 1

            windows_blades += 1
            windows_core += bcore
            blade_dict['windows_farm'][bprofile]['core'] += bcore

            if blade['BladeUse.slotsinuse'] >= 1:
                win_active += 1
                blade_dict['windows_farm'][bprofile]['active'] += 1
            else:
                win_idle += 1
                blade_dict['windows_farm'][bprofile]['idle'] += 1

        #OTHER
        else:
            others += 1

    # Change the Ottawa farm name from windows to ottawa.
    if site == 'ottawa':
        farm = site
    else:
        farm = "windows"

    linux_info = {
        "timestamp": TIME,
        "metric_type": "blade_status",
        "farm": "linux",
        "total": (linux_blades + wrk_active_blades + lin_aws), # + AWS
        "active": (lin_active + wrk_active_blades + lin_aws_active), # + AWS
        "idle": (lin_idle + lin_aws_idle), # + AWS
        "core": linux_core}

    windows_info = {
        "timestamp": TIME,
        "metric_type": "blade_status",
        "farm": farm,
        "total": (windows_blades + win_aws),
        "active": (win_active + win_aws_active),
        "idle": (win_idle + win_aws_idle),
        "core": windows_core}

    LOG_FILE.write("{}\n".format(json.dumps(linux_info)))
    LOG_FILE.write("{}\n".format(json.dumps(windows_info)))

    for profile in blade_dict['windows_farm']:
        profile_dict = {
            "timestamp": TIME,
            "metric_type": "blade_profiles",
            "farm": farm,
            "profile": profile,
            "total": blade_dict['windows_farm'][profile]['total'],
            "active": blade_dict['windows_farm'][profile]['active'],
            "idle": blade_dict['windows_farm'][profile]['idle'],
            "core": blade_dict['windows_farm'][profile]['core']}
        LOG_FILE.write("{}\n".format(json.dumps(profile_dict)))
    for profile in blade_dict['linux_farm']:
        profile_dict = {
            "timestamp": TIME,
            "metric_type": "blade_profiles",
            "farm": "linux",
            "profile": profile,
            "total": blade_dict['linux_farm'][profile]['total'],
            "active": blade_dict['linux_farm'][profile]['active'],
            "idle": blade_dict['linux_farm'][profile]['idle'],
            "core": blade_dict['linux_farm'][profile]['core']}
        LOG_FILE.write("{}\n".format(json.dumps(profile_dict)))

    return linux_blades, windows_blades

def get_show_usage(site, lin_blades, win_blades):
    """Get the current number of tasks that are active on each show."""

    active_tasks = tq.tasks("active",
                            columns=['Job.projects', 'Blade.profile'],
                            limit=0)

    allocations = tu.get_farm_allocations(site)

    show_active_dict = {}
    for task in active_tasks:
        if task['Job.projects']:
            show = task['Job.projects'][0]
            if len(show) == 3:
                show = show.upper()
            if show not in show_active_dict.keys():
                show_active_dict[show] = {'active': 1,
                                          'local': 0, 'aws': 0, 'wrk': 0}
            else:
                show_active_dict[show]['active'] += 1

            if 'Workstation' in task['Blade.profile'] or 'Linux_nimby' in task['Blade.profile']:
                show_active_dict[show]['wrk'] += 1
            elif 'AWS' in task['Blade.profile']:
                show_active_dict[show]['aws'] += 1
            elif 'Linux' in task['Blade.profile'] or 'Windows' in task['Blade.profile'] or 'Desktop' in task['Blade.profile']:
                show_active_dict[show]['local'] += 1


    for show in show_active_dict:
        if len(show) == 3:
            farm = "linux"
            local_blades = lin_blades
        else:
            farm = "windows"
            local_blades = win_blades

        farm_key = "{}_farm".format(farm)
        show_alloc = allocations[farm_key].get(show, '')
        if show_alloc:
            show_blades = int(local_blades * show_alloc)
        else:
            show_blades = 0

        # Change the Ottawa farm name from windows to ottawa.
        if site == 'ottawa':
            farm = site

        active_aws = show_active_dict[show]['aws']
        active_wrk = show_active_dict[show]['wrk']
        show_tot_blades = show_blades + active_aws + active_wrk
        show_dict = {
            "timestamp": TIME,
            "metric_type": "show_usage",
            "show": show,
            "farm": farm,
            "active": show_active_dict[show]['active'],
            "share": show_tot_blades,
            "tractor_allocation": show_alloc}
        LOG_FILE.write("{}\n".format(json.dumps(show_dict)))


def __main__():

    for site in ['vancouver', 'ottawa']:
        # Connect to the correct Tractor Engine
        if site == 'vancouver':
            tu.connect_to_vancouver_tq()
        elif site == 'ottawa':
            tu.connect_to_ottawa_tq()


        lin_blades, win_blades = get_blade_status(site)

        get_show_usage(site, lin_blades, win_blades)


__main__()
