#!/sw/bin/python

import json
import tractor.api.query as tq
from datetime import datetime
import sys

sys.path.insert(0, '/sw/pipeline/rendering/renderfarm-tools')
from tractor_utils import tractor_utils as tu

TODAY = datetime.today().strftime('%m%d%Y')
TIME_NOW = datetime.now()
TIME = TIME_NOW.strftime('%Y-%m-%d %H:%M:%S')
# FILE_DIR = "/mnt/w/PubLogs/render/datadog_done_error_{}.log".format(TODAY)
FILE_DIR = "/home/gaguero/Documents/TEST_TMP/datadog_done_error_{}.log".format(
    TODAY)
LOG_FILE = open(FILE_DIR, "a")

def main(site):
    """ Main function that retrieves data from Tractor and creates a dictionary
        of project information for a specific site. The dictionary contains
        information about the project's status, errors, what Farm it belongs to,
        and timestamps.

        Parameters:
            site (str): the name of the site to retrieve data from. Helps
            identify what Farm is being used as well.

        Returns:
            None
    """

    general_tasks = tq.invocations(
        'stoptime > -5m', columns=['Job.projects', 'Job.title', 'rcode',
                                   'stoptime', 'jid'], limit=0)

    projects_dict = {}

    for task in general_tasks:
        # If the job has a Project value of None then it will simply be skipped,
        # otherwise an entry is generated.
        try:
            task_project = task['Job.projects'][0]
        except TypeError:
            continue

        if len(task_project) == 3:
            task_project = task_project.upper()
        #  If the project is not yet in the dictionary, it creates a key named
        #  after the project and a value of another dictionary being initialized
        #  with no errors and no tasks done.
        if task_project not in projects_dict.keys():
            projects_dict[task_project] = {'done': 0, 'error': 0}
            # Adding Farm name according to the length of the Project
            if site == 'ottawa':
                projects_dict[task_project]['farm'] = 'ottawa'
            else:
                if len(task_project) == 3:
                    projects_dict[task_project]['farm'] = 'linux'
                else:
                    projects_dict[task_project]['farm'] = 'windows'

            projects_dict[task_project]['show'] = task_project
            projects_dict[task_project]['timestamp'] = TIME
            projects_dict[task_project]['error_jids'] = []

        #  If RCode is zero then the tasks was completed with no errors
        if task['rcode'] == 0:
            projects_dict[task_project]['done'] += 1
        else:
            projects_dict[task_project]['error'] += 1

            if task['jid'] not in projects_dict[task_project]['error_jids']:
                projects_dict[task_project]['error_jids'].append(task['jid'])

    write_to_file(projects_dict)

def write_to_file(projects_dict):
    """ Writes the "projects_dict" to a log file which is then used
        by Datadog to display the generated data.

        Parameters:
            projects_dict (dict): A dictionary containing project names as keys
            and their corresponding done count, error count, farm, show,
            and timestamp as values.

        Returns:
            None
    """

    for key in projects_dict:
        LOG_FILE.write("{}\n".format(json.dumps(projects_dict[key])))

if __name__ == "__main__":

    for site in ['vancouver', 'ottawa']:
        # Connect to the correct Tractor Engine
        if site == 'vancouver':
            tu.connect_to_vancouver_tq()
            main(site)
        elif site == 'ottawa':
            tu.connect_to_ottawa_tq()
            main(site)

