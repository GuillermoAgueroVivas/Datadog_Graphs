#!/sw/bin/python

# This script's job is to be able to check is a Blade has a note on itself indicating it has failed more than 5 times, therefor assigning a flag to it right away.
# Works hand in hand with the "blade_health_checker" script.

import json
import tractor.api.query as tq
import tqauth
from datetime import datetime

TODAY = datetime.today().strftime('%m%d%Y')
TIME_NOW = datetime.now()
TIME = TIME_NOW.strftime('%Y-%m-%d %H:%M:%S')
FILE_DIR = "/mnt/w/PubLogs/render/datadog_blade_flags_{}.log".format(TODAY)

def main():
    """Starts off everything, and it then creates the final dictionary that will be added to the log file used by Datadog."""

    all_blades = get_blades()
    final_blade_dict = pull_data_and_sort(all_blades)
    log_file = open(FILE_DIR, "a")
    farm = "linux"
    
    for blade in final_blade_dict:
        blade_info = {'metric_type': 'blade_note_check',
                      'timestamp': TIME,
                      'farm': farm,
                      'blade': final_blade_dict[blade]['blade_name'],
                      'profile': final_blade_dict[blade]['blade_profile'],
                      'blade_flags': final_blade_dict[blade]['flags']}
        # Adds this Blade to the log file already generated by the "blades_health_checker" script.
        log_file.write("{}\n".format(json.dumps(blade_info)))

def get_blades():
    """Gets all the wanted Blades with the "error accrual hiatus" note.

        Returns:
            blades_dict (dict): A dictionary with all the affected Blades.
    """
    all_blades = tq.blades("status like 'error'", columns=['name', 'profile'], limit=0)
    blades_dict = []

    if all_blades:
        return all_blades
        # for blade in all_blades:
        #     blades_dict.append(blade)
        # print(blades_dict)
    else:
        # print("No Blades right now")
        exit()

def pull_data_and_sort(blades_dict):
    """Goes through every blade in the given list. It then adds a 'flag' to the Blade Dictionary for each Blade.

                Parameters:
                    blades_dict (list): list of all wanted blades

                Returns:
                    new_blades_dict (dict): Dictionary containing all Blades with the amount of flags for each Blade.
    """
    new_blades_dict = {}

    for blade in blades_dict:
        bn = blade["name"]  # Blade Name
        bp = blade["profile"]  # Blade Profile

        if blade not in new_blades_dict.keys():
            # Initializing each Blade
            new_blades_dict[bn] = {'blade_name': bn, 'blade_profile': bp, 'flags': 0}

        new_blades_dict[bn]['flags'] += 1

    return new_blades_dict

if __name__ == "__main__":
    tq.setEngineClientParam(user=tqauth.USERNAME, password=tqauth.PASSWORD)
    main()
