#!/bin/bash

rm -r /dev/shm/lamellar_*  2> /dev/null #cleanup incase any previous run failed unexpectedly
#echo "$HOSTNAME"
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

NPROC=`nproc --all`
ENDPE=$(( $NUMPES-1))
JOBID=$((1+ $RANDOM % 100 ))
S_CORE=$((0))
E_CORE=$(($S_CORE + $THREADS))
for pe in $(seq 0 $ENDPE)
do
if [ "$E_CORE" -gt "$NPROC" ]; then
echo "more threads than cores"
exit
fi
#outfile=${pe}_shmem_test.out
#let
echo "$pe $S_CORE $E_CORE $NPROC"
LAMELLAE_BACKEND="shmem" LAMELLAR_THREADS=${THREADS:-$((NPROC/NUMPES))} LAMELLAR_NUM_PES=$NUMPES LAMELLAR_PE_ID=$pe LAMELLAR_JOB_ID=$JOBID taskset --cpu-list $S_CORE-$E_CORE $bin "${@:2}" & 
S_CORE=$(($E_CORE + 1 ))
E_CORE=$(($S_CORE + $THREADS))
done
#LAMELLAE_BACKEND="shmem" LAMELLAR_THREADS=${THREADS:-$((NPROC/NUMPES))} LAMELLAR_NUM_PES=$NUMPES LAMELLAR_PE_ID=0 LAMELLAR_JOB_ID=$JOBID taskset --cpu-list 0-15 $bin "${@:2}" &
#LAMELLAE_BACKEND="shmem" LAMELLAR_THREADS=${THREADS:-$((NPROC/NUMPES))} LAMELLAR_NUM_PES=$NUMPES LAMELLAR_PE_ID=1 LAMELLAR_JOB_ID=$JOBID taskset --cpu-list 15-32 $bin "${@:2}" &
# LAMELLAR_THREADS=${THREADS:-$((NPROC/NUMPES))} 
# echo "$LAMELLAR_THREADS ${THREADS} $NPROC $((NPROC/NUMPES))"
wait
