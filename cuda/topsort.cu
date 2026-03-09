/*
 * topsort.cu — Distributed Kahn's topological sort kernels.
 *
 * Three kernels, matching FfiKernelFn<u32>:
 *   topsort_init_ffi              — memcpy host in-degrees → device
 *   topsort_find_frontier_ffi     — scan in-degrees, emit zero-degree vertices
 *   topsort_apply_decrements_ffi  — decrement in-degrees by index list
 *
 * Extra per-PE state (device-side globals set by host helpers before dispatch):
 *   d_base_offset      global vertex base for this PE
 *   d_frontier_buf     output: global vertex IDs added this wave
 *   d_frontier_count   atomic write counter into d_frontier_buf
 *   d_frontier_cap     allocated capacity of d_frontier_buf
 *   d_decrements       input: local vertex indices to decrement
 *   d_num_decrements   count of entries in d_decrements
 *
 * Init state (host-side globals, safe because each PE is its own process):
 *   g_init_src_host    pointer to the host in-degree array
 *   g_init_src_n       element count
 *
 * Dispatch order each BFS wave
 * ────────────────────────────
 *   Wave A — find frontier
 *     1. topsort_set_base_offset(base)
 *     2. topsort_reset_frontier(dev_frontier_ptr, cap)
 *     3. FfiKernel "topsort_find_frontier" → topsort_find_frontier_ffi
 *     4. CudaStream::synchronize
 *     5. topsort_get_frontier_count(→ h_count)
 *     6. cudaMemcpy frontier_buf[0..h_count] → host → push_to FRONTIER_OUT
 *
 *   Wave B — fan-out (host-side, sends DecrementAm for remote vertices)
 *
 *   Wave C — apply decrements
 *     1. topsort_set_decrements(dev_dec_ptr, n)
 *     2. FfiKernel "topsort_apply_decrements" → topsort_apply_decrements_ffi
 *     3. CudaStream::synchronize
 */

#include <cuda_runtime.h>
#include <stdint.h>
#include <stddef.h>

#define PROCESSED_MARKER 0xFFFFFFFFu

/* ── Device-side globals ─────────────────────────────────────────────────── */

static __device__ uint64_t       d_base_offset    = 0;
static __device__ uint64_t*      d_frontier_buf   = NULL;
static __device__ unsigned int   d_frontier_count = 0;
static __device__ size_t         d_frontier_cap   = 0;
static __device__ uint64_t*      d_decrements     = NULL;
static __device__ size_t         d_num_decrements = 0;

/* ── Host-side init globals (per-process) ────────────────────────────────── */

static unsigned int* g_init_src_host = NULL;
static size_t        g_init_src_n    = 0;

/* ── Kernels ─────────────────────────────────────────────────────────────── */

/*
 * find_frontier_impl — one thread per vertex.
 *
 * Checks if in_degrees[tid] == 0.  If so:
 *   • atomically claims a slot in d_frontier_buf,
 *   • writes the global vertex ID (d_base_offset + tid),
 *   • marks the in-degree as PROCESSED_MARKER.
 *
 * Threads whose in-degree is PROCESSED_MARKER (already handled in a previous
 * wave) or > 0 are no-ops.
 */
__global__ void find_frontier_impl(unsigned int* in_degrees, size_t n)
{
    size_t tid = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    if (tid >= n) return;
    if (in_degrees[tid] != 0) return;

    /* Claim a slot; discard if we exceed the buffer capacity. */
    unsigned int slot = atomicAdd(&d_frontier_count, 1u);
    if (slot < (unsigned int)d_frontier_cap) {
        d_frontier_buf[slot] = d_base_offset + (uint64_t)tid;
    }
    in_degrees[tid] = PROCESSED_MARKER;
}

/*
 * apply_decrements_impl — one thread per decrement entry.
 *
 * Atomically subtracts 1 from in_degrees[d_decrements[tid]], skipping
 * vertices already marked PROCESSED_MARKER.  Because the graph is a DAG
 * and each edge is decremented exactly once, atomicSub never underflows.
 */
__global__ void apply_decrements_impl(unsigned int* in_degrees, size_t n)
{
    size_t tid = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    if (tid >= d_num_decrements) return;

    size_t local_idx = (size_t)d_decrements[tid];
    if (local_idx >= n) return;
    if (in_degrees[local_idx] == PROCESSED_MARKER) return;

    atomicSub(&in_degrees[local_idx], 1u);
}

/* ── Host helpers ────────────────────────────────────────────────────────── */

extern "C" int topsort_set_init_data(const unsigned int* host_ptr, size_t n)
{
    g_init_src_host = (unsigned int*)host_ptr;
    g_init_src_n    = n;
    return 0;
}

extern "C" int topsort_set_base_offset(uint64_t base)
{
    return (int)cudaMemcpyToSymbol(d_base_offset, &base, sizeof(base));
}

/*
 * topsort_reset_frontier — zero the atomic counter and (re)set the output
 * buffer pointer + capacity for the upcoming wave.
 */
extern "C" int topsort_reset_frontier(uint64_t* dev_ptr, size_t cap)
{
    cudaError_t err;
    unsigned int zero = 0;
    err = cudaMemcpyToSymbol(d_frontier_count, &zero,    sizeof(zero));
    if (err != cudaSuccess) return (int)err;
    err = cudaMemcpyToSymbol(d_frontier_buf,   &dev_ptr, sizeof(dev_ptr));
    if (err != cudaSuccess) return (int)err;
    err = cudaMemcpyToSymbol(d_frontier_cap,   &cap,     sizeof(cap));
    return (int)err;
}

/*
 * topsort_get_frontier_count — copy d_frontier_count from device to host.
 * Call after find_frontier kernel + stream synchronize.
 */
extern "C" int topsort_get_frontier_count(unsigned int* out)
{
    return (int)cudaMemcpyFromSymbol(out, d_frontier_count, sizeof(unsigned int));
}

extern "C" int topsort_set_decrements(uint64_t* dev_ptr, size_t n)
{
    cudaError_t err;
    err = cudaMemcpyToSymbol(d_decrements,     &dev_ptr, sizeof(dev_ptr));
    if (err != cudaSuccess) return (int)err;
    err = cudaMemcpyToSymbol(d_num_decrements, &n,       sizeof(n));
    return (int)err;
}

/* ── FfiKernelFn-compatible entry points ─────────────────────────────────── */

/*
 * topsort_init_ffi — copies host in-degrees into the device buffer.
 *
 * Uses cudaMemcpyAsync on the provided stream so it is ordered with
 * subsequent kernel launches on the same stream.
 * Requires topsort_set_init_data() to have been called first.
 */
extern "C" int topsort_init_ffi(
    unsigned int* device_ptr,
    size_t        n,
    unsigned int  gx, unsigned int gy, unsigned int gz,
    unsigned int  bx, unsigned int by, unsigned int bz,
    unsigned int  shared_mem,
    void*         stream)
{
    (void)gx; (void)gy; (void)gz;
    (void)bx; (void)by; (void)bz;
    (void)shared_mem;

    if (g_init_src_host == NULL || g_init_src_n != n) return 1;
    return (int)cudaMemcpyAsync(
        device_ptr, g_init_src_host, n * sizeof(unsigned int),
        cudaMemcpyHostToDevice, (cudaStream_t)stream);
}

/*
 * topsort_find_frontier_ffi — grid covers the vertex count (n).
 * Requires topsort_set_base_offset() and topsort_reset_frontier() first.
 */
extern "C" int topsort_find_frontier_ffi(
    unsigned int* in_degrees,
    size_t        n,
    unsigned int  gx, unsigned int gy, unsigned int gz,
    unsigned int  bx, unsigned int by, unsigned int bz,
    unsigned int  shared_mem,
    void*         stream)
{
    dim3 grid(gx, gy, gz);
    dim3 block(bx, by, bz);
    find_frontier_impl<<<grid, block, shared_mem, (cudaStream_t)stream>>>(
        in_degrees, n);
    return (int)cudaGetLastError();
}

/*
 * topsort_apply_decrements_ffi — grid covers d_num_decrements.
 * Requires topsort_set_decrements() first.
 */
extern "C" int topsort_apply_decrements_ffi(
    unsigned int* in_degrees,
    size_t        n,
    unsigned int  gx, unsigned int gy, unsigned int gz,
    unsigned int  bx, unsigned int by, unsigned int bz,
    unsigned int  shared_mem,
    void*         stream)
{
    dim3 grid(gx, gy, gz);
    dim3 block(bx, by, bz);
    apply_decrements_impl<<<grid, block, shared_mem, (cudaStream_t)stream>>>(
        in_degrees, n);
    return (int)cudaGetLastError();
}
