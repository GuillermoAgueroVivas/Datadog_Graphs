#!/sw/bin/python

#  This script is to be able to show how many how many jobs are in the farm per department (LGT, SHD, etc.)

import json
import tractor.api.query as tq
import tqauth
from datetime import datetime

TODAY = datetime.today().strftime('%m%d%Y')
TIME_NOW = datetime.now()
TIME = TIME_NOW.strftime('%Y-%m-%d %H:%M:%S')
FILE_DIR = "/mnt/w/PubLogs/render/test_jobs_per_department_{}.log".format(TODAY)
LOG_FILE = open(FILE_DIR, "a")

def main():
    """Does the main search of all the jobs and iterates through all of them to get the needed information"""

    general_jobs = tq.jobs("not done and not pausetime", columns=['projects', 'projects', 'title'], limit=0)

    jobs_dict = {}

    # Anim, Lighting, Shading, Modeling, Comp.
    for job in general_jobs:
        job_project = job['projects'][0]
        job_title = job['title']

        department = find_dept(job_title)

        # if department == 'OTHER':
        #     print job_title

        if len(job_project) == 3:
            job_project = job_project.upper()

        # If the project is not yet in the dictionary, it creates a key named after the project and a value of another dictionary being initialized with a total of 0,
        # show name and a timestamp.
        if job_project not in jobs_dict.keys():
            jobs_dict[job_project] = {'total': 0}
            jobs_dict[job_project]['show'] = job_project
            jobs_dict[job_project]['timestamp'] = TIME
        # If the department is not included in the Show then it adds it and initializes it with a value of 0
        if department not in jobs_dict[job_project].keys():
            jobs_dict[job_project][department] = 0
        # Adds 1 for the total and one for the corresponding Department
        jobs_dict[job_project][department] += 1
        jobs_dict[job_project]['total'] += 1

    write_to_file(jobs_dict)

def find_dept(jtitle):
    """Takes the job title and returns the department it's part of"""

    if ("_LGT.v" in jtitle and ".katana" in jtitle
            or "_LGT_" in jtitle and ".v" in jtitle and ".katana" in jtitle
            or "Savior for" in jtitle or "Auto Seq Render" in jtitle
            or "_CMP.v" in jtitle and ".katana" in jtitle):
        return "LGT"

    elif ("Cache Export" in jtitle or "QC Template" in jtitle
          or "QC Render" in jtitle or "QC Katana" in jtitle
          or "_ANM.v" in jtitle or "Publish Face" in jtitle
          or "Face Projection" in jtitle or "QC katana" in jtitle
          or "Anim Auto" in jtitle or "RIG_RigAnim" in jtitle
          or "FaceProjection" in jtitle
          or "_LAY.v" in jtitle and ".ma" in jtitle
          or "Yeti Auto Shot" in jtitle or "XGen Batcher:" in jtitle
          or "CFX Model Builder" in jtitle
          or "Harmony Render" in jtitle and "ANM" in jtitle):
        return "ANM"

    elif ("CMP" in jtitle and ".nk" in jtitle or "NUKE Comper" in jtitle
          or ".nk_Write" in jtitle or "Nuke Comp" in jtitle or
          "After Effects" in jtitle and "CMP" in jtitle or
          "CMP" in jtitle):
        return "CMP"

    elif ("_SHD_Shading" in jtitle or "Nuke Turntable" in jtitle
          or "_SHD_" in jtitle and "katana" in jtitle
          or "_MDL_" in jtitle and "katana" in jtitle
          or "_TXT_" in jtitle and "katana" in jtitle
          or "_FX" in jtitle and ".nk" in jtitle):
        return "SHD"

    elif ("_MDL_" in jtitle and "katana" in jtitle
          or "_MDL_" in jtitle
          or "Katana Turntable" in jtitle or "Env Turntable" in jtitle):
        return "MDL"

    elif ("Trigger Job" in jtitle or "EDL Subclip" in jtitle
          or "EDL Media" in jtitle or "MFD Deletion" in jtitle
          or "DMP publish" in jtitle or "Asset Packer" in jtitle
          or "JetStream" in jtitle or "Jetstream" in jtitle or "Task Status Updater:" in jtitle
          or "TJ-T" in jtitle or "CreateShotgunVersion" in jtitle
          or "Duplicate versions" in jtitle or "Convert Delivery" in jtitle
          or "Create Client" in jtitle or "Renderfarm Render Report" in jtitle
          or "Render Reports" in jtitle):
        return "ADMIN"

    elif ("Houdini" in jtitle or
          "Harmony Render" in jtitle and "FX" in jtitle):
        return "FX"

    else:
        return "OTHER"

def write_to_file(jobs_dict):

    for key in jobs_dict:
        LOG_FILE.write("{}\n".format(json.dumps(jobs_dict[key])))

if __name__ == "__main__":
    tq.setEngineClientParam(user=tqauth.USERNAME, password=tqauth.PASSWORD)
    main()
