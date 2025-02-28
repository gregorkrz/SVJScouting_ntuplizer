import argparse

argparse.argument("--input", type=str, help="Input file with listed cmsRun commands")
argparse.argument("--n-jobs", type=int, default=50)

args = argparse.parse_args()

with open(args.input, "r") as f:
    cmds = f.readlines()

job_template = f"""
universe = vanilla
Executable = jobExecCondorByLine.sh
+REQUIRED_OS = "rhel7"
+DesiredOS = REQUIRED_OS
request_disk = 1000000
request_memory = 8000
request_cpus = 4
Should_Transfer_Files = YES
WhenToTransferOutput = ON_EXIT
Transfer_Input_Files = jobExecCondorByLine.sh
Output = {out}.stdout
Error = {err}.stderr
Log = {log}.condor
notification = Never
x509userproxy = $ENV(X509_USER_PROXY)
Arguments = -S step1.sh,step2.sh  -I -C CMSSW_10_6_29_patch1 -X root://t3se01.psi.ch:1094/store/user/gkrzmanc/jetclustering/sim/26Feb_KeepAll -j step_RECO_s-channel_mMed-1400_mDark-20_rinv-0.7_alpha-peak_13TeV-pythia8_n-10 -p $(Process) -o root://t3se01.psi.ch:1094/store/user/gkrzmanc/jetclustering/sim/26Feb_KeepAll/RECO
want_graceful_removal = true
on_exit_remove = (ExitBySignal == False) && (ExitCode == 0)
on_exit_hold = ( (ExitBySignal == True) || (ExitCode != 0) )
on_exit_hold_reason = strcat("Job held by ON_EXIT_HOLD due to ",\
        ifThenElse((ExitBySignal == True), "exit by signal", \
strcat("exit code ",ExitCode)), ".")
job_machine_attrs = "GLIDEIN_CMSSite"

Requirements = HAS_SINGULARITY == True
+AvoidSystemPeriodicRemove = True
Queue Process in {queue}

"""

# launch the condor jobs
for i in range(0, len(cmds), args.max_cmds_per_file):
    with open(f"condor_{i}.sh", "w") as f:
        f.write("#!/bin/bash\n")
        for j in range(args.max_cmds_per_file):
            if i+j < len(cmds):
                f.write(cmds[i+j])
    os.system(f"chmod +x condor_{i}.sh")
    os.system(f"condor_submit condor_{i}.sh")
