import argparse
# args: --num-parts, --part and --input
parser = argparse.ArgumentParser(description='Split a file into parts')
parser.add_argument('--num-parts', type=int, help='Number of parts to split the file into') #
parser.add_argument('--part', type=int, help='Which part to extract')
parser.add_argument('--input', type=str, help='Input file to split') # /work/gkrzmanc/jetclustering/code/ntupl_cmds_27feb.txt
args = parser.parse_args()
# read the file
with open(args.input, "r") as f:
    lines = f.readlines()
# split the file into parts
n = len(lines)
part_size = n // args.num_parts
start = (args.part) * part_size
end = ( args.part+1) * part_size
if args.part == args.num_parts:
    end = n
part = lines[start:end]
print("Executing part {} of {} with {} lines".format(args.part, args.num_parts, len(part)))
print("(lines start: {}, end: {})".format(start, end))
import os
# execute cmds. Print the stdout and stderr of cmds
for line in part:
    print("executing cmd", line)
    os.system(line.strip())
print("Done")
