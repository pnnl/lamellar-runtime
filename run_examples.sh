#!/bin/bash


target_dir=/home/scratch/$USER
output_dir=/home/scratch/$USER
target_dir=$PWD/target
root=$PWD
. $root/../junction-prep.rc


## test using local lamellae 
# mkdir -p local_lamellae
# cd local_lamellae
# for toolchain in stable ; do #nightly; do
#  features=""
#  if [ "${toolchain}" = "nightly" ]; then
#    features="--features nightly"
#  fi
# #  cargo clean
#  cargo +$toolchain build --release ${features} --examples
#  mkdir -p ${toolchain}
#  cd ${toolchain}
#  for dir in `ls $root/examples`; do
#    mkdir -p $dir 
#    cd $dir
#      for test in `ls $root/examples/$dir`; do
#        test=`basename $test .rs`
#        LAMELLAR_THREADS=19 srun -N 1 --partition=all --time 0:5:00 $root/target/release/examples/$test > ${test}.out 2>&1  &
#      done
#    cd ..
#  done
#  cd ..
#  wait
# done


### test using rofi shm lamellae
# mkdir -p shmem_lamellae
# cd shmem_lamellae
# for toolchain in stable; do
#  features=""
# #  if [ "${toolchain}" = "nightly" ]; then
# #    features="--features nightly"
# #  fi
# #  cargo clean
# #  cargo +$toolchain build --release --examples
#  mkdir -p ${toolchain}
#  cd ${toolchain}
#  for mode in debug release; do
#   mkdir -p $mode
#   cd ${mode}
#   for dir in `ls $root/examples`; do
#     mkdir -p $dir
#     cd $dir
#       for test in `ls $root/examples/$dir`; do
#         test=`basename $test .rs`
#         LAMELLAR_MEM_SIZE=$((5 * 1024 * 1024 * 1024)) srun -n 1 -N 1 -A lamellar --partition=datavortex --time 0:5:00 --mpi=pmi2 $root/lamellar_run.sh -N=1 -T=23 $root/target/${mode}/examples/$test |& tee ${test}_n1.out &
#         LAMELLAR_MEM_SIZE=$((5 * 1024 * 1024 * 1024)) srun -n 1 -N 1 -A lamellar --partition=datavortex --time 0:5:00 --mpi=pmi2 $root/lamellar_run.sh -N=2  -T=11 $root/target/${mode}/examples/$test |& tee ${test}_n2.out &
#         LAMELLAR_MEM_SIZE=$((5 * 1024 * 1024 * 1024)) srun -n 1 -N 1 -A lamellar --partition=datavortex --time 0:5:00 --mpi=pmi2 $root/lamellar_run.sh -N=8 -T=2 $root/target/${mode}/examples/$test |& tee ${test}_n8.out &
#       done
#     cd ..
#   done
#   cd ..
#   wait
#  done
#  cd ..
# done

local_results_dir=async_backends
results_dir=${output_dir}/rofiverbs_lamellae/${local_results_dir}
### test using rofi verbs lamellae
rm -r ${results_dir}

rm -r rofiverbs_lamellae
mkdir -p rofiverbs_lamellae
mkdir -p ${results_dir}
ln -s ${output_dir}/rofiverbs_lamellae rofiverbs_lamellae

cd rofiverbs_lamellae/${local_results_dir}
for toolchain in stable; do #nightly; do
  features=""
  if [ "${toolchain}" = "nightly" ]; then
    features="--features nightly"
  fi
  # cargo clean


  # cargo +$toolchain build --release --features enable-rofi  --examples
  mkdir -p ${toolchain}
  cd ${toolchain}
  for mode in release ; do
    # cargo +$toolchain build --$mode --features enable-rofi  --examples
    mkdir -p $mode    
    cd ${mode}

    for dir in `ls $root/examples`; do
      mkdir -p $dir
      cd $dir

        # for test in `ls $root/examples/$dir`; do
        #   test=`basename $test .rs`
        #   echo "performing ${test}"
        #   LAMELLAE_BACKEND="rofi" LAMELLAR_ROFI_PROVIDER="verbs" LAMELLAR_THREADS=63 srun --cpus-per-task=64 --cpu-bind=ldoms,v -N 2 --time 0:5:00 --mpi=pmi2 $root/target/release/examples/$test > ${test}_n2.out 2>&1 & 
        # done
        sbatch --exclude=j004,j005,j036 --cpus-per-task=64 -N 2 --time 0:120:00 $root/batch_runner.sh $root $dir $mode 64 2 $target_dir
        if [ $dir != "bandwidths" ]; then
          sbatch --exclude=j004,j005,j036 --cpus-per-task=64 -N 8 --time 0:120:00 $root/batch_runner.sh $root $dir $mode 64 8 $target_dir
          sbatch --exclude=j004,j005,j036 --cpus-per-task=32 -N 16 -n 32 --time 0:240:00 $root/batch_runner.sh $root $dir $mode 32 32 $target_dir
                
        #   for test in `ls $root/examples/$dir`; do
        #     test=`basename $test .rs`
        #     echo "performing ${test}"
        #     LAMELLAE_BACKEND="rofi" LAMELLAR_ROFI_PROVIDER="verbs" LAMELLAR_THREADS=63 srun --cpus-per-task=64 --cpu-bind=ldoms,v -N 8 --time 0:5:00 --mpi=pmi2 $root/target/release/examples/$test > ${test}_n8.out 2>&1 &
        #   done
        #   for test in `ls $root/examples/$dir`; do
        #     test=`basename $test .rs`
        #     echo "performing ${test}"
        #     LAMELLAE_BACKEND="rofi" LAMELLAR_ROFI_PROVIDER="verbs" LAMELLAR_THREADS=31 srun --cpus-per-task=32 --cpu-bind=ldoms,v -n 32 -N 16  --time 0:10:00 --mpi=pmi2 $root/target/release/examples/$test > ${test}_n32.out 2>&1 &
        #   done
        fi
      cd ..
      cur_tasks=`squeue -u frie869 | grep " R " | wc -l`
      while [ $cur_tasks -gt 3 ]; do
        cur_tasks=`squeue -u frie869 | grep " R " | wc -l`
        sleep 5
      done
      
    done
    cd ..
    wait
  done
  cd ..
done
# #
