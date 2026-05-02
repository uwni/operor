// The VISA library uses `__int64` in some APIs, but this type is not defined
// by default in Zig's C translator. Define it before including visa.h.
#define __int64 long long
#include "visa.h"
