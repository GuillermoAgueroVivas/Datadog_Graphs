#!/sw/bin/python

import json
import tractor.api.query as tq
import tqauth
from datetime import datetime

TODAY = datetime.today().strftime('%m%d%Y')
TIME_NOW = datetime.now()
TIME = TIME_NOW.strftime('%Y-%m-%d %H:%M:%S')
FILE_DIR = "/mnt/w/PubLogs/render/datadog_blade_flags_{}.log".format(TODAY)

def main():
    """ Main function that creates a dictionary of Blade information with flags that have been raised according to the amount of time the Blade took in comparison to others.
        This information will then be added to a log file used by Datadog."""

    jobs_dict = get_jobs()
    blade_dict = pull_data_and_sort(jobs_dict)
    log_file = open(FILE_DIR, "a")
    farm = "linux"

    for blade in blade_dict:
        if blade_dict[blade]['flags'] > 0:
            blade_info = {'metric_type': 'blade_flags',
                          'timestamp': TIME,
                          'farm': farm,
                          'blade': blade_dict[blade]['blade_name'],
                          'profile': blade_dict[blade]['blade_profile'],
                          'completed_tasks': blade_dict[blade]['completed_lgt_tasks'],
                          'blade_flags': blade_dict[blade]['flags']}
            log_file.write("{}\n".format(json.dumps(blade_info)))
        else:
            continue

def get_jobs():
    """ Gets all the wanted LGT jobs which have finished within the passed hour.

        Returns:
            jobs_dict (dict): A dictionary with all the Job IDs
    """

    all_jobs = tq.jobs("stoptime > -2h and title like 'LGT' and numtasks > 10 and not pausetime and numtasks = numdone", columns=['jid'], limit=0)
    jobs_dict = []

    for job in all_jobs:
        jobs_dict.append(job['jid'])

    return jobs_dict

def pull_data_and_sort(jobs_dict):
    """ Goes through every job in the given list, iterates through the job's tasks while creating a big dictionary of all Blades. Then it creates another dictionary
        containing the core time taken per task with the Blade name. It then identifies which task took the longest
        while taking note of the Blade name. Finally, adds a 'flag' to the Blade Dictionary according to the Blade name that took the longest in each job.

        Parameters:
            jobs_dict (list): list of all wanted jobs

        Returns:
            blade_dict (dict): Dictionary containing all Blades with the amount of completed tasks and flags for each Blade.
    """

    blade_dict = {}
    for job in jobs_dict:

        jid_query = 'jid=' + str(job)
        job_tasks = tq.invocations(jid_query, columns=['Blade.name', 'Blade.numslots', 'Blade.numcpu', 'Task.title', 'jid', 'elapsedreal', 'Blade.profile'], limit=0)  # "current and" may need to be potentially used

        # Creating the Show Dictionary
        initialized_frames = {}
        s_time = 0

        for task in job_tasks:
            blade_name = ""

            if 'Katana_Render' in task['Task.title']:
                frame = get_frame(task['Task.title'])
                # Getting Standardized Time
                s_time = standardized_time(task['elapsedreal'], task['Blade.numslots'], task['Blade.numcpu'])
                initialized_frames[frame] = {"blade_name": task['Blade.name'], "standardized_time": s_time}

                if task['Blade.name']:
                    blade_name = task['Blade.name']
                    blade_profile = task['Blade.profile']
                    if blade_name not in blade_dict.keys():
                        # Initializing each Blade
                        blade_dict[blade_name] = {'blade_name': blade_name, 'blade_profile': blade_profile, 'completed_lgt_tasks': 0, 'flags': 0}
                # Adding a +1 to completed tasks every time the same Blades comes up.
                blade_dict[blade_name]['completed_lgt_tasks'] += 1

        # Getting the highest, the second-highest and the third-highest times inside the 'initialized_frames' dictionary
        highest_time, second_highest_time, third_highest_time = get_largest_times(initialized_frames)

        too_low = 115200  # This number is equal to 15 minutes standardized in seconds, so if the highest time took less than 15 minutes then it will be skipped on the flag process.

        if highest_time < too_low:
            # print("Too low. Job: {}".format(job))
            continue

        average = get_average(initialized_frames)
        control_number = average * 2

        highest_blade, s_highest_blade = get_highest_blades(highest_time, second_highest_time, control_number, initialized_frames)

        if highest_blade in blade_dict.keys():
            blade_dict[highest_blade]['flags'] += 1

        if s_highest_blade in blade_dict.keys():
            blade_dict[s_highest_blade]['flags'] += 1

    return blade_dict

def get_frame(task_title):
    """ Gets the first frame that will then be used to find the ones that follow it.

        Parameters:
            task_title (str): The task's title

        Returns:
            frame (int): Frame-number by itself
    """

    split = task_title.split(" ")
    frame = int(split[1])
    return frame

def standardized_time(elapsed_time, numslots, numcpu):
    """ Takes in a Blade Profile and the elapsed time of a frame to standardize the time to a 72c Blade.

        Parameters:
            elapsed_time (float): time elapsed from start to end of a task.
            numslots (int): The total number of slots on the blade
            numcpu (int): The number of cpus/cores of the host.

        Returns:
            s_time (float): Standardized time
    """
    if numslots == 0:
        numslots = 1

    # We half the numcpu and the majority of out blades have 2 threads per core
    # Tractor displays the thread count not the real core count.
    real_numcpu = numcpu / 2

    # Our 128c and most of the low-end workstations are not hyper-threaded.
    if numcpu == 128 or numcpu <= 12:
        real_numcpu = numcpu

    s_time = elapsed_time * (real_numcpu / numslots)
    # print("{} {} {}.".format(elapsed_time, real_numcpu, numslots))

    return s_time

def get_largest_times(initialized_frames):
    """ Takes in the dictionary generated with all the 'Initialized Frames' for the job. It then adds all times to a new list and after sorting it, it returns the top 3 highest times.

        Parameters:
            initialized_frames (dict): all Initialized Frames with their standardized time.

        Returns:
            highest_time (float): Highest time in the given list
            second_highest_time (float): Second-highest time in the given list
            third_highest_time (float): Third-highest time in the given list
    """

    job_s_times = []  # List for all the standardized times inside the initialized_frames
    for s_time in initialized_frames.keys():
        job_s_times.append(initialized_frames[s_time]["standardized_time"])

    job_s_times.sort()

    highest_time = job_s_times[-1]
    second_highest_time = job_s_times[-2]
    third_highest_time = job_s_times[-3]

    return highest_time, second_highest_time, third_highest_time

def get_average(initialized_frames):
    """ Takes in the dictionary generated with all the 'Initialized Frames' for the job. After creating a list of these times, it returns the average time taken.

        Parameters:
            initialized_frames (dict): all Initialized Frames with their standardized time.

        Returns:
            average_time (float): Average time taken by all tasks in the given list of times (which belong to all tasks in the given job).
    """

    job_s_times = []  # List for all the standardized times inside the initialized_frames
    for s_time in initialized_frames.keys():
        job_s_times.append(initialized_frames[s_time]["standardized_time"])

    sum_job_s_times = sum(job_s_times)
    average = sum_job_s_times / len(job_s_times)

    return average

def get_highest_blades(highest_time, second_highest_time, control_number, initialized_frames):
    """ This function returns the blades with the highest and second highest standardized time value, given a control number and a dictionary of initialized frames.

        Parameters:
            highest_time (int): The highest standardized time value.
            second_highest_time (int): The second highest standardized time value.
            control_number (int): The control number used to compare against the standardized time values.
            initialized_frames (dict): A dictionary of initialized frames.

        Returns:
            highest_blade (str): The blade name with the highest standardized time value that is greater than the control number.
            s_highest_blade (str): The blade name with the second highest standardized time value that is greater than the control number.
    """

    highest_blade = None
    s_highest_blade = None

    for i_frame in initialized_frames.keys():
        # Standardized Time for this frame
        st = initialized_frames[i_frame]["standardized_time"]

        # If the highest time belongs to this frame and the number is higher than the control number then this deserves a flag, therefore the highest_blade is given a value
        if highest_time > control_number and highest_time == st:
            highest_blade = initialized_frames[i_frame]['blade_name']
            # print(highest_time, st, control_number)

        # If the second-highest time belongs to this frame and the number is higher than the control number then this deserves a flag, therefore the s_highest_blade is given a value
        if second_highest_time > control_number and second_highest_time == st:
            s_highest_blade = initialized_frames[i_frame]['blade_name']
            # print(second_highest_time, st, control_number)

    return highest_blade, s_highest_blade

if __name__ == "__main__":

    tq.setEngineClientParam(user=tqauth.USERNAME, password=tqauth.PASSWORD)
    main()
