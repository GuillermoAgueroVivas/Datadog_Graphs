# Datadog Graphs:
I created every single script in here to gather certain information, create multiple dictionaries, and add them to specific file locations so they can be taken in by Datadog and viewed as different types of graphs or visuals. This way the data can be monitored and compared through different periods of time throughout the day(s).  

**blade_error_note_check.py**

It checks whether a server blade has a specific note indicating more than five failures. The script collaborates with the "blade_health_checker" script to efficiently assess server health. It compiles the findings into a dictionary, subsequently contributing to a log file utilized by Datadog for comprehensive monitoring. The generated log entries include essential details such as the timestamp, farm, blade name, profile, and the count of flags associated with each blade.

**blades_health_checker.py**

**erred_done_tasks_getter.py**

**get_current_blade_stats.py**

**harmony_active_waiting_getter.py**

**harmony_done_getter.py**

**jobs_per_department_getter.py**

**machine_stats_getter.py**
