#!/bin/bash


# target_dir=/home/scratch/$USER
target_dir=$PWD/target
output_dir=/home/scratch/$USER

root=$PWD
. $root/../junction-prep.rc

local_results_dir=async_backends
results_dir=${output_dir}/rofiverbs_lamellae/${local_results_dir}
### test using rofi verbs lamellae
rm -r ${results_dir}

rm -r rofiverbs_lamellae
mkdir -p ${results_dir}
ln -s ${output_dir}/rofiverbs_lamellae rofiverbs_lamellae


cargo build --release --features enable-rofi --features tokio-executor --examples -j 20


cd rofiverbs_lamellae/${local_results_dir}
for toolchain in stable; do #nightly; do
  features=""
  if [ "${toolchain}" = "nightly" ]; then
    features="--features nightly"
  fi

  mkdir -p ${toolchain}
  cd ${toolchain}
  for mode in release ; do
    mkdir -p $mode    
    cd ${mode}

    for dir in `ls $root/examples`; do
      mkdir -p $dir
      cd $dir
        sbatch --exclude=j004,j005,j036 --cpus-per-task=64 -N 2 --time 0:120:00 $root/batch_runner.sh $root $dir $mode 64 2 $target_dir
        if [ $dir != "bandwidths" ]; then
          sbatch --exclude=j004,j005,j036 --cpus-per-task=64 -N 8 --time 0:120:00 $root/batch_runner.sh $root $dir $mode 64 8 $target_dir
          sbatch --exclude=j004,j005,j036 --cpus-per-task=32 -N 16 -n 32 --time 0:240:00 $root/batch_runner.sh $root $dir $mode 32 32 $target_dir
        fi
      cd ..
      sleep 2
      cur_tasks=`squeue -u frie869 | wc -l`
      running_tasks=`squeue -u frie869 | grep " R " | wc -l`
      while [ $((cur_tasks+running_tasks)) -gt 6 ]; do
        cur_tasks=`squeue -u frie869 | wc -l`
        running_tasks=`squeue -u frie869 | grep " R " | wc -l`
        sleep 5
      done   
      # fi   
    done
    cd ..
    wait
  done
  cd ..
done
# #
