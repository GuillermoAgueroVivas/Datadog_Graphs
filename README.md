# Datadog Graphs:
I created every single script in here to gather certain information, create multiple dictionaries, and add them to specific file locations so they can be taken in by Datadog and viewed as different types of graphs or visuals. This way the data can be monitored and compared through different periods of time throughout the day(s).  

**main_farm_selection_window.py**) allows for a selection of what section of the Farm you wish to modify. This list is auto-generated from the '.config' file in case any section is removed or added.
- Second window (depending on the selection, either **linuxfarm_window.py** or **windowsfarm_window.py** will run) displays a list of all available shows in the selected Farm section together with a slider and a combo box for each one showing the current percentage value individually. Here you can adjust the values and proceed to the next window or cancel and go back to selected another section of the Farm. There is also a check to make sure that the values do not go above 100%.
- The third window is a confirmation window (**changes_confirmation_window.py**) which displays all the changes made in the previous window versus the current values from the '.config' file.
- Last Window (**changes_applied_window.py**) will allow the user to stage and push the changes to the '.config' file, choose to go back to the first window and make more changes (this will create a temporary '.config' file) or simply exit and discard all changes.

After the changes have been submitted, the terminal running the script will display a multiple messages related to the success of the tool changing the '.config' file and reloading Tractor while comparing the values to the ones that are currently live. 

**Please note that for this UI to work in a different environment, a '.config' file is necessary as well as changing the paths required in the first window**
