#!/bin/bash

target_dir=$PWD/target/
output_dir=/home/scratch/$USER
root=$PWD

local_results_dir=v0.8.0
lamellae_dir=libfabric_ucx_lamellae
results_dir=${output_dir}/${lamellae_dir}/${local_results_dir}

### test using rofi verbs lamellae
rm -r ${results_dir}
rm -r ${lamellae_dir}
mkdir -p ${results_dir}
ln -s ${output_dir}/${lamellae_dir} ${lamellae_dir}

cd ${lamellae_dir}/${local_results_dir}
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
          sbatch --exclude=j004,j005,j036 --cpus-per-task=32 -N 8 -n 16 --time 0:120:00 $root/batch_runner.sh $root $dir $mode 32 16 $target_dir
          sbatch --exclude=j004,j005,j036 --cpus-per-task=4 -N 16 -n 256 --time 0:240:00 $root/batch_runner.sh $root $dir $mode 4 256 $target_dir
        fi
      cd ..
      sleep 2
      cur_tasks=`squeue -u frie869 | grep frie869 |wc -l`
      running_tasks=`squeue -u frie869 | grep frie869| grep " R " | wc -l`
      while [ $((cur_tasks+running_tasks)) -gt 6 ]; do
        cur_tasks=`squeue -u frie869 | grep frie869 | wc -l`
        running_tasks=`squeue -u frie869 | grep frie869 | grep " R " | wc -l`
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
