#!/sw/bin/python

import psutil as ps
import socket
from datetime import datetime

# Script made by Guillermo Aguero - Render TD

TODAY = datetime.today().strftime('%m%d%Y')
TIME_NOW = datetime.now()
TIME = TIME_NOW.strftime('%Y-%m-%d %H:%M:%S')
BLADE_OWNER = socket.gethostname()  # What Blade is running the script. Keep in mind this name will be on the log file names and in Datadog as a Facet value.
FILE_DIR = "/mnt/w/PubLogs/render/{}_machine_stats_{}.log".format(BLADE_OWNER, TODAY)  # Creation of file
LOG_FILE = open(FILE_DIR, "a")

# Make dictionaries inside the main dictionary for all different sections (memory, disk usage and CPU)
def main(blade_owner):
    """ Returns a dictionary containing information about the overall CPU usage, memory usage, and disk usage of the specified blade owner.

        Parameters:
            blade_owner (str): The name of the blade owner for whom the usage information is being retrieved.

        Returns:
            all_info_dict (dict): A dictionary containing the CPU usage information, memory usage information, and disk usage information of the specified blade owner.
    """

    cpu_info_dict = get_cpu_info(blade_owner)  # Get CPU usage information
    memory_info_dict = get_memory_info(blade_owner)  # Get memory usage information
    disk_info_dict = get_disk_info(blade_owner)  # Get disk usage information

    all_info_dict = {"overall_CPU_usage": cpu_info_dict, "memory_usage": memory_info_dict, "disk_usage": disk_info_dict}

    return all_info_dict

def get_cpu_info(blade_owner):
    """ This function returns a dictionary containing CPU usage information for a specified blade owner.

        Parameters:
            blade_owner (str): The name of the blade owner for which the CPU usage information is being retrieved.

        Returns:
            cpu_info_dict (dict): A dictionary containing the overall CPU usage percentage, CPU frequency in MHz,
            percentage average CPU load for the last 5 minutes, timestamp of the retrieval, and the blade owner's name.
    """

    # Overall CPU
    cpu_info_dict = {"overall_cpu_percent": round(ps.cpu_percent(3), 2)}

    # CPU Frequency in Mhz
    cpu_freq = ps.cpu_freq()
    cpu_info_dict["cpu_min_freq"] = cpu_freq.min
    cpu_info_dict["cpu_max_freq"] = cpu_freq.max
    cpu_info_dict["cpu_current_freq"] = round(cpu_freq.current, 2)

    # Percentage Average CPU Load for the last 5 minutes
    avg_cpu_load = [cpu_load / ps.cpu_count() * 100 for cpu_load in ps.getloadavg()]
    cpu_info_dict["avg_cpu_load_percent"] = round(avg_cpu_load[1], 2)

    cpu_info_dict["timestamp"] = TIME
    cpu_info_dict["blade_owner"] = blade_owner

    return cpu_info_dict

def get_memory_info(blade_owner):
    """ This function retrieves memory usage information related to RAM in Bytes and returns a dictionary containing the total memory, used memory, and percent used memory.

        Parameters:
            blade_owner (str): the name of the owner of the blade being monitored.

        Returns:
            memory_dict (dict): A dictionary containing the total memory, used memory, percent used memory, timestamp, and blade_owner.
    """

    # Get memory usage information (related to RAM in Bytes)
    memory_dict = {}
    memory = ps.virtual_memory()

    memory_dict["total_memory"] = round(to_gb(memory.total), 2)
    memory_dict["used_memory"] = round(to_gb(memory.used), 2)
    mem_percent = 100 * memory_dict["used_memory"] / memory_dict["total_memory"]
    memory_dict["percent_used_memory"] = round(mem_percent, 2)

    memory_dict["timestamp"] = TIME
    memory_dict["blade_owner"] = blade_owner

    return memory_dict

def get_disk_info(blade_owner):
    """ Get disk usage information for a specified blade owner.

        Parameters:
            blade_owner (str): Name of the blade owner

        Returns:
            disk_dict (dict): A dictionary containing disk usage information, including total disk space, free disk space, used disk space, and used disk space percentage.
    """
    # Get disk usage information
    disk_dict = {}
    disk = ps.disk_usage('/')
    disk_dict["total_disk_space"] = round(to_gb(disk.total), 2)
    disk_dict["free_disk_space"] = round(to_gb(disk.free), 2)
    disk_dict["used_disk_space"] = round(to_gb(disk.used), 2)
    disk_dict["used_disk_space_percent"] = disk.percent

    disk_dict["timestamp"] = TIME
    disk_dict["blade_owner"] = blade_owner

    return disk_dict

def to_gb(value):
    """ Converts a value from Bytes to Gigabytes.

        Parameters:
            value (int): The value to be converted from Bytes to Gigabytes.

        Returns:
        gb (float): The converted value in Gigabytes.
    """

    gb = value / 1e+9
    return gb

def write_to_file(blade_stats_dict):
    """ Writes the values of the dictionary into a log file, one line for each key value pair.

        Parameters:
            blade_stats_dict (dict): Dictionary containing key-value pairs.

        Returns:
            None
    """

    for stat in blade_stats_dict.keys():
        LOG_FILE.write("{}\n".format(blade_stats_dict[stat]))

if __name__ == "__main__":
    final_dict = main(BLADE_OWNER)
    write_to_file(final_dict)

