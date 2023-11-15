import sys
import tractor.api.query as tq
import tqauth
import json
import re
from datetime import datetime

sys.path.insert(0, '/sw/pipeline/rendering/renderfarm-tools')
from tractor_utils import tractor_utils as tu

TODAY = datetime.today().strftime('%m%d%Y')
TIME_NOW = datetime.now()
TIME = TIME_NOW.strftime('%Y-%m-%d %H:%M:%S')
FILE_DIR = "/mnt/w/PubLogs/render/datadog_harmonytaskdone_{}.log".format(TODAY)

def main(site):
    """ Main function that iterates through a dictionary of shows and writes JSON-formatted log entries to a file. Each entry records the number of render tasks completed for a particular show, episode or shot.

        Parameters:
            site (str): the name of the site where the function is being run ('ottawa' or 'windows').

        Returns:
            None
    """

    show_dict = pull_data_and_sort()

    if site == 'ottawa':
        farm = 'ottawa'
    else:
        farm = 'windows'

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
        for shot in show_dict[show]['shot']:
            show_shot = {'metric_type': 'shot_ren_tasks',
                         'timestamp': TIME,
                         'farm': farm,
                         'show': show,
                         'episode': show_dict[show]['shot'][shot]['epi'],
                         'shot': shot,
                         'done': show_dict[show]['shot'][shot]['ren_done']}
            log_file.write("{}\n".format(json.dumps(show_shot)))

def pull_data_and_sort():
    """ This function pulls data on Harmony Render tasks, filters them based on criteria, and sorts them into a dictionary with information on the shots, episodes and renders done for each show.

        Parameters:
            None

        Returns:
            show_dict (dict): a dictionary containing information on the renders done for each show, including shot, episode, and render counts.
    """

    ren_tasks = tq.tasks("Job.title like 'Harmony Render:' and "
                         "Task.title like 'Batch Render:' and "
                         "done and stoptime > -1h",
                         columns=['Job.title', 'title', 'jid', 'Job.projects', 'Job.numtasks', 'Job.priority', 'state'],
                         limit=0, archive=True)

    # Creating the Show Dictionary
    show_dict = {}

    for task in ren_tasks:
        if task['Job.projects']:
            show = task['Job.projects'][0]
            if show not in show_dict.keys():
                # Initializing a show with a spot for Shot, Episode and Renders Done.
                show_dict[show] = {'shot': {}, 'seq': {}, 'epi': {}, 'ren_done': 0}

            shot = get_shot(task['Job.title'])
            epi = get_episode(task['Job.title'])

            if epi not in show_dict[show]['epi'].keys():
                show_dict[show]['epi'][epi] = {'ren_done': 0}
            if shot not in show_dict[show]['shot'].keys():
                show_dict[show]['shot'][shot] = {'ren_done': 0, 'epi': epi}

            show_dict[show]['ren_done'] += 1
            show_dict[show]['epi'][epi]['ren_done'] += 1
            show_dict[show]['shot'][shot]['ren_done'] += 1

    return show_dict

def get_shot(title):
    """Extracts the shot number from a given title using regular expressions.

        Parameters:
            title (str): The title to extract the shot number from.

        Returns:
            shot (str): The extracted shot number, or an empty string if no shot number was found.
    """

    shot = re.findall('[A-Z]+[0-9]+_[A-Z][0-9]+_[A-Z]+[0-9]+[A-Z][0-9]+', title)
    if not shot:
        shot = re.findall('[A-Z]+[0-9]+_[A-Z][0-9]+_[A-Z]+[0-9]+[A-Z]+', title)
        # print 1
    if not shot:
        shot = re.findall('[A-Z]+[0-9]+_[A-Z][0-9]+_[A-Z]+[0-9]+', title)
        # print 2
    if not shot:
        shot = re.findall('[A-Z]+[0-9]+_[A-Z][0-9]+[A-Z]_[A-Z]+[0-9]+', title)
        # print 3
    if not shot:
        shot = re.findall('[A-Z]+_[A-Z][0-9]+_[A-Z]+[0-9]+', title)
        # print 4
    if not shot:
        shot = re.findall('[A-Z]+[0-9]+_[A-Z]+[0-9]+_[A-Z]+[0-9]+', title)
        # print 5
    if not shot:
        shot = re.findall('[A-Z]+[0-9]+_[A-Z]+[0-9]+', title)
        # print 6
    if not shot:
        shot = re.findall('[A-Z]+[0-9][0-9]+_[A-Z]+[0-9]+', title)
        # print 7

    if not shot:
        print "Couldn't find shot from: '{}'".format(title)
        shot = [""]

    return shot[0]

def get_episode(title):
    """ Extracts the episode from a given title by using regular expression.

        Parameters:
            title (str): The title from which the episode needs to be extracted.

        Returns:
            episode (str): The episode number extracted from the given title. If no episode number is found, returns an empty string.
    """

    epi = re.findall('_(.*?)_', title)

    if not epi:
        epi = re.findall('_([A-Z][0-9]+)_', title)

    if epi:
        episode = epi[0]
    else:
        episode = ''

    return episode

if __name__ == "__main__":

    for site in ['vancouver', 'ottawa']:
        # Connect to the correct Tractor Engine
        if site == 'vancouver':
            tu.connect_to_vancouver_tq()
            main(site)
        elif site == 'ottawa':
            tu.connect_to_ottawa_tq()
            main(site)
