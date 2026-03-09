/*
 * histo.cu — GPU histogram kernel using 64-bit atomicAdd.
 *
 * Design
 * ──────
 * The Rust FfiKernelFn signature only passes (ptr, len, grid, block, shared,
 * stream).  To also pass the update-index buffer we store a device-side
 * pointer + count in CUDA __device__ globals that are written before each
 * kernel dispatch via histo_set_updates().  Because each PE is a separate OS
 * process (Lamellar SMP backend), these globals are truly per-PE.
 *
 * Call order on each PE per wave
 * ─────────────────────────────
 *   1. [host] histo_set_updates(dev_update_ptr, num_updates)
 *   2. [host] FfiCudaKernel "histo_u64" → histo_kernel_u64_ffi()
 *              which launches histo_increment<<<grid, block, 0, stream>>>
 *   3. [host] CudaStream::synchronize()
 */

#include <cuda_runtime.h>
#include <stdint.h>
#include <stddef.h>

/* ── Device-side globals ─────────────────────────────────────────────────── */

static __device__ unsigned long long* d_updates     = NULL;
static __device__ size_t             d_num_updates  = 0;

/* ── Kernel ──────────────────────────────────────────────────────────────── */

/*
 * histo_increment — one thread per update.
 *
 * Each thread reads one update offset from d_updates and atomically
 * increments the corresponding histogram bucket.
 *
 * For best performance on sm_80+ set the grid to cover d_num_updates with
 * a block size of 256.  The bounds check makes any extra threads harmless.
 */
__global__ void histo_increment(unsigned long long* counts, size_t counts_len)
{
    size_t tid = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    if (tid >= d_num_updates) return;

    size_t offset = (size_t)d_updates[tid];
    if (offset < counts_len) {
        atomicAdd(&counts[offset], 1ULL);
    }
}

/* ── Host helpers ────────────────────────────────────────────────────────── */

/*
 * histo_set_updates — write the device-side update-buffer pointer and count.
 *
 * Must be called (on the host, before the kernel) with a device pointer that
 * remains valid until the kernel completes.  Returns cudaSuccess (0) on
 * success or a CUDA error code otherwise.
 */
extern "C" int histo_set_updates(unsigned long long* device_ptr, size_t num_updates)
{
    cudaError_t err;
    err = cudaMemcpyToSymbol(d_updates,    &device_ptr,  sizeof(device_ptr));
    if (err != cudaSuccess) return (int)err;
    err = cudaMemcpyToSymbol(d_num_updates, &num_updates, sizeof(num_updates));
    return (int)err;
}

/* ── FfiKernelFn-compatible entry point ─────────────────────────────────── */

/*
 * histo_kernel_u64_ffi — conforms to FfiKernelFn<u64>.
 *
 * The grid should be sized to cover num_updates (not counts_len):
 *   grid_x = ceil(num_updates / block_x)
 *
 * Returns 0 on success or the CUDA error code of the last API call.
 */
extern "C" int histo_kernel_u64_ffi(
    unsigned long long* counts,
    size_t              counts_len,
    unsigned int gx, unsigned int gy, unsigned int gz,
    unsigned int bx, unsigned int by, unsigned int bz,
    unsigned int shared_mem,
    void*        stream)
{
    dim3 grid(gx, gy, gz);
    dim3 block(bx, by, bz);
    histo_increment<<<grid, block, shared_mem, (cudaStream_t)stream>>>(
        counts, counts_len);
    return (int)cudaGetLastError();
}
