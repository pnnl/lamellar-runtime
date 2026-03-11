#include "wrapper.h"

ucc_status_t ucc_collective_test_wrapper(ucc_coll_req_h request)
{
    return ucc_collective_test(request);
}
