#!/bin/bash
echo "$HOSTNAME"
NUMPES=1

for i in "$@"; do
  case $i in
    -N=*|--numpes=*)
    NUMPES="${i#*=}"
    shift
    ;;
    -T=*|--threads-per-pe=*)
    THREADS="${i#*=}"
    shift
    ;;
  esac
done

bin=$1

NPROC=`nproc`
ENDPE=$(( $NUMPES-1))
JOBID=$((1+ $RANDOM % 100 ))
for pe in $(seq 0 $ENDPE)
do
outfile=${pe}_shmem_test.out
# echo "${THREADS:-$((NPROC/NUMPES))} $NUMPES $pe $JOBID" > ${outfile}
# LAMELLAR_MEM_SIZE=$((20*1024*1024*1024)) 
# LAMELLAR_THREADS=${THREADS:-$((NPROC/NUMPES))} LAMELLAR_NUM_PES=$NUMPES LAMELLAR_PE_ID=$pe LAMELLAR_JOB_ID=$JOBID gdb --ex run --ex "thread apply all bt"  --ex quit --args  $bin "${@:2}" &> >(tee -a ${outfile}) &
LAMELLAE_BACKEND="shmem" LAMELLAR_THREADS=${THREADS:-$((NPROC/NUMPES))} LAMELLAR_NUM_PES=$NUMPES LAMELLAR_PE_ID=$pe LAMELLAR_JOB_ID=$JOBID $bin "${@:2}" & #&> >(tee -a ${outfile}) &

done

wait