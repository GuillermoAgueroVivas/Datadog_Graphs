# Datadog Graphs:
I created every single script here to gather certain information, create multiple dictionaries, and add them to specific file locations so they can be taken in by Datadog and viewed as different types of graphs or visuals. This way the data can be monitored and compared through different periods of time throughout the day(s). All of these are meant to be used as part of a Crontab specifying how often these scripts would run.  

- **blade_error_note_check.py**

  - It checks whether a server blade has a specific note indicating more than five failures. The script collaborates with the "blade_health_checker" script to efficiently assess server health. It compiles the findings into a dictionary, subsequently contributing to a log file utilized by Datadog for comprehensive monitoring. The generated log entries include essential details such as the timestamp, farm, blade name, profile, and the count of flags associated with each blade.

- **blades_health_checker.py**
  - It focuses on completed tasks within the past hour, identifying blades that took longer than average. The script standardizes completion times based on blade specifications, providing insights into potential performance issues. The results, including blade names, profiles, completed tasks, and flags, are logged for monitoring and analysis by Datadog.   

- **erred_done_tasks_getter.py**
  - It captures data on completed and error tasks within the last 5 minutes, categorizing them by project, farm, and timestamp. The collected information is logged in a file, ready for consumption by Datadog for real-time monitoring and analysis.  

- **harmony_active_waiting_getter.py**
  -  Extracts and logs real-time data on active and waiting Harmony render tasks. It organizes the information by show, episode, and shot, creating a structured JSON log for each category. This tool aids in monitoring rendering backlogs, helping teams manage workload distribution efficiently. 

**harmony_done_getter.py**

**jobs_per_department_getter.py**

**machine_stats_getter.py**
