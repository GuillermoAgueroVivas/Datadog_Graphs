import argparse
import tractor.api.author as author

PARSER = argparse.ArgumentParser(description='Write LGT/ANM Stats to DataDog')
PARSER.add_argument('-s', '--shows', type=str, help='Shows.')
PARSER.add_argument('-b', '--breakdown', type=str, help="Type of Breakdown")
PARSER.add_argument('-e', '--epi_timeframe', type=str, help="all_epi or daily_epi")

PARSER.add_argument('-lgt', action='store_true', help="LGT Show Shot Breakdown")
PARSER.add_argument('-anm', action='store_true', help="ANM Show Shot Breakdown")

ARGS = PARSER.parse_args()
SHOWS = [str(s) for s in ARGS.shows.split(',')]
BREAKDOWN = "-{}".format(ARGS.breakdown)
EPI_TIMEFRAME = "-{}".format(ARGS.epi_timeframe)



if ARGS.lgt:
    SCRIPT = "/sw/pipeline/rendering/datadog/epi_seq_shot_stats_LGT.py"
    DEPT = "LGT"
elif ARGS.anm:
    SCRIPT = "/sw/pipeline/rendering/datadog/epi_seq_shot_stats_ANM.py"
    DEPT = "ANM"

job = author.Job(title=("Datadog: {} Episode_Sequence_Shot_Stats {} {}"
                        "".format(DEPT, BREAKDOWN, EPI_TIMEFRAME)),
                 projects=["RND"],
                 priority=9000,
                 service="Linux64,Overhead",
                 tier="admin",
                 maxactive=1,
                 comment="NoKill")

for show in SHOWS:

    command = ["/sw/bin/python",
               SCRIPT, "-s", show, BREAKDOWN, EPI_TIMEFRAME]
    job.newTask(title="{} {} Stats".format(show, DEPT),
                argv=command)

new_jid = job.spool()
