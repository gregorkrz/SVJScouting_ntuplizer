# the input filelist comes from ntuplizer_command_gen.py script from the SVJ clustering repo
import argparse
import os
parser = argparse.ArgumentParser(description='Submit jobs to the cluster')
#argparse.argument("--input", type=str, help="Input file with listed cmsRun commands")
#argparse.argument("--n-jobs", type=int, default=10)
#argparse.argument("--no-submit", "-ns", action="store_true", help="Do not submit the slurm job")

parser.add_argument("--input", type=str, help="Input file with listed cmsRun commands", default="/work/gkrzmanc/jetclustering/code/filelist.pkl") #/work/gkrzmanc/jetclustering/code/ntupl_cmds_27feb.txt
parser.add_argument("--filelist", "-fl", action="store_true") # if true, it will use a pickle filelist instead of the txt file for input
parser.add_argument("--n-parts", type=int, default=5)
parser.add_argument("--no-submit", "-ns", action="store_true", help="Do not submit the slurm job")

args = parser.parse_args()
assert args.filelist
#with open(args.input, "r") as f:
#    cmds = f.readlines()

import pickle
filelist = pickle.load(open(args.input, "rb"))
filelist_keys = sorted(filelist.keys())
# Launch the slurm jobs
for i in range(len(filelist_keys)):
    #cmd = "bash -c 'source /cvmfs/cms.cern.ch/cmsset_default.sh && cmsenv && python PhysicsTools/SVJScouting/exec_lines.py --num-parts {} --input {} --part {}'".format(args.n_jobs, args.input, i)
    for p in range(args.n_parts):
        input_fl = ""
        input_fl_list = []
        input_fl_xrdcp = []
        part_min_idx = p * len(filelist[filelist_keys[i]]) // args.n_parts
        part_max_idx = (p+1) * len(filelist[filelist_keys[i]]) // args.n_parts
        for c, j in enumerate(sorted(list(filelist[filelist_keys[i]]))):
            if c < part_min_idx or c >= part_max_idx:
                continue # for debugging
            input_fl += " inputFiles=file:$TMPDIR/input/"+os.path.basename(j) + " "
            input_fl_list += [j]
            input_fl_xrdcp += ["xrdcp -f " + j + " $TMPDIR/input/ --verbose"]
        xrdcp_input = "\n".join(input_fl_xrdcp)
        output_fl = "$TMPDIR/output/PFNano_" + filelist_keys[i] + "_part_{}.root".format(p)
        output_filename = "PFNano_" + filelist_keys[i] + "_part_{}.root".format(p)
        cms_cmd = "cmsRun PhysicsTools/SVJScouting/test/ScoutingNanoAOD_fromMiniAOD_cfg.py " + input_fl + " outputFile=" + output_fl + " maxEvents=-1 isMC=true era=2018 signal=True"
        cmd1 = "bash -c 'source /cvmfs/cms.cern.ch/cmsset_default.sh && cmsenv && " + cms_cmd + "'"
        if not os.path.exists("jobs/logs"):
            os.makedirs("jobs/logs")
        with open("jobs/logs/launch_{}_{}.sh".format(i, p), "w") as f:
            err = "jobs/logs/launch_{}_{}_err.log".format(i,p )
            log = "jobs/logs/launch_{}_{}_log.log".format(i, p)
            job_template = f"""#!/bin/bash
#SBATCH --partition=standard           # Specify the partition
#SBATCH --account=t3                  # Specify the account
#SBATCH --mem=10000                   # Request 10GB of memory
#SBATCH --time=05:00:00              # Set the time limit to 1 hour
#SBATCH --error={err}         # Redirect stderr to a log file
#SBATCH --output={log}         # Redirect stderr to a log file
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=gkrzmanc@student.ethz.ch

export TMPDIR=/scratch/${{USER}}/tmp/${{SLURM_JOB_ID}}

mkdir -p $TMPDIR/input
mkdir -p $TMPDIR/output
{xrdcp_input}
echo 'xrdcp done'
echo 'Listing the contents:'
ls $TMPDIR/input
export APPTAINER_TMPDIR=/work/gkrzmanc/singularity_tmp
export APPTAINER_CACHEDIR=/work/gkrzmanc/singularity_cache
cd /work/gkrzmanc/CMSSW_10_6_26/src
ls $TMPDIR/output
srun singularity exec -B /work/gkrzmanc -B /cvmfs -B /pnfs -B /scratch docker://cmssw/el7:x86_64 {cmd1}
echo 'ls $tmpdir'
ls $TMPDIR
echo '......'
ls $TMPDIR/output
echo 'Done - now copying the output'
echo 'Now running xrdcp' >&2
cp  {output_fl} /work/gkrzmanc/jetclustering/data/Feb26_2025_E1000_N500_noPartonFilter_GluonFix/{output_filename}
echo 'Copied'
rm -rf $TMPDIR
echo 'removed the tmp dirs'
            """
            f.write(job_template)
        print("Wrote to jobs/logs/launch_{}_{}.sh".format(i, p))
        if not args.no_submit:
            os.system("sbatch jobs/logs/launch_{}_{}.sh".format(i, p))

# /work/gkrzmanc/jetclustering/code/filelist.pkl
